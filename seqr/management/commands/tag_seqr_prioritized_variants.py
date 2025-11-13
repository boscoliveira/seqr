from collections import defaultdict
from datetime import datetime
from django.contrib.postgres.aggregates import ArrayAgg
from django.core.management.base import BaseCommand
from django.db.models import Q, F
from django.db.models.functions import JSONObject
import json
import os

from clickhouse_backend.models import ArrayField, StringField
from clickhouse_search.backend.fields import NamedTupleField
from clickhouse_search.backend.functions import ArrayFilter, ArrayMap
from clickhouse_search.search import get_search_queryset, get_transcripts_queryset, clickhouse_genotypes_json, \
    get_data_type_comp_het_results_queryset, SAMPLE_DATA_FIELDS, SELECTED_GENE_FIELD
from panelapp.models import PaLocusListGene
from reference_data.models import GENOME_VERSION_GRCh38
from seqr.models import Project, Family, Individual, Sample, LocusList
from seqr.utils.communication_utils import send_project_notification
from seqr.utils.gene_utils import get_genes
from seqr.utils.search.utils import clickhouse_only, get_search_samples, COMPOUND_HET
from seqr.views.utils.orm_to_json_utils import SEQR_TAG_TYPE
from seqr.views.utils.variant_utils import bulk_create_tagged_variants, gene_ids_annotated_queryset
from settings import SEQR_SLACK_DATA_ALERTS_NOTIFICATION_CHANNEL

import logging
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('project')

    @clickhouse_only
    def handle(self, *args, **options):
        with open(f'{os.path.dirname(__file__)}/../../fixtures/seqr_high_priority_searches.json', 'r') as file:
            config = json.load(file)

        family_guid_map = {}
        family_name_map = {}
        project = Project.objects.get(guid=options['project'])
        for db_id, guid, family_id in Family.objects.filter(project=project).values_list('id', 'guid', 'family_id'):
            family_guid_map[guid] = db_id
            family_name_map[db_id] = family_id

        exclude_genes = get_genes(config['exclude']['gene_ids'], genome_version=GENOME_VERSION_GRCh38)
        gene_by_moi = defaultdict(dict)
        for gene_list in config['gene_lists']:
            self._get_gene_list_genes(gene_list['name'], gene_list['confidences'], gene_by_moi, exclude_genes.keys())

        family_variant_data = defaultdict(lambda: {'matched_searches': set(), 'matched_comp_het_searches': set(), 'support_vars': set()})
        search_counts = {}
        for dataset_type, searches in config['searches'].items():
            self._run_dataset_type_searches(
                dataset_type, searches, family_variant_data, search_counts, family_guid_map,
                project, exclude_genes, gene_by_moi, exclude=config['exclude'],
            )

        today = datetime.now().strftime('%Y-%m-%d')
        new_tag_keys, num_updated, num_skipped = bulk_create_tagged_variants(
            family_variant_data, tag_name=SEQR_TAG_TYPE, get_metadata=self._get_metadata(today, 'matched_searches'),
            get_comp_het_metadata=self._get_metadata(today, 'matched_comp_het_searches'), user=None, remove_missing_metadata=False,
        )

        family_variants = defaultdict(list)
        for family_id, variant_id in family_variant_data.keys():
            family_variants[family_id].append(variant_id)
        logger.info(f'Tagged {len(new_tag_keys)} new and {num_updated} previously tagged variants in {len(family_variants)} families, found {num_skipped} unchanged tags:')
        for search_name, count in search_counts.items():
            logger.info(f'  {search_name}: {count} variants')
        if not new_tag_keys:
            return

        family_new_counts = defaultdict(int)
        for family_id, variant_id in new_tag_keys:
            family_new_counts[family_id] += 1

        send_project_notification(
            project,
            notification=f'{len(new_tag_keys)} new seqr prioritized variants',
            subject='New prioritized variants tagged in seqr',
            email_template='This is to notify you that {notification} have been tagged in seqr project {project_link}',
            slack_channel=SEQR_SLACK_DATA_ALERTS_NOTIFICATION_CHANNEL,
            slack_detail='\n'.join(sorted([
                f'{family_name_map[family_id]}: {count} new tags' for family_id, count in family_new_counts.items()
            ])),
        )

    @classmethod
    def _run_dataset_type_searches(cls, dataset_type, searches, family_variant_data, search_counts, family_guid_map, project, exclude_genes, gene_by_moi, exclude):
        is_sv = dataset_type == Sample.DATASET_TYPE_SV_CALLS
        sample_qs = get_search_samples([project]).filter(dataset_type=dataset_type)
        if is_sv:
            sample_qs = sample_qs.exclude(individual__sv_flags__contains=['outlier_num._calls'])
        sample_types = list(sample_qs.values_list('sample_type', flat=True).distinct())
        assert len(sample_types) == 1
        sample_type = sample_types[0]
        if is_sv:
            dataset_type = f'{dataset_type}_{sample_type}'
        samples_by_family = {
            family_guid: samples for family_guid, samples in sample_qs.values('individual__family__guid').annotate(
                samples=ArrayAgg(JSONObject(**SAMPLE_DATA_FIELDS, maternal_guid='individual__mother__guid', paternal_guid='individual__father__guid'))
            ).values_list('individual__family__guid', 'samples')
            if any(s['affected'] == Individual.AFFECTED_STATUS_AFFECTED for s in samples)
        }

        logger.info(f'Searching for prioritized {dataset_type} variants in {len(samples_by_family)} families in project {project.name}')
        for search_name, config_search in searches.items():
            exclude_locations = not config_search.get('gene_list_moi')
            search_genes = exclude_genes if exclude_locations else gene_by_moi[config_search['gene_list_moi']]
            sample_data = cls._get_valid_family_sample_data(
                project, sample_type, samples_by_family, config_search.get('family_filter'),
            )
            run_search_func = cls._run_comp_het_search if config_search['inheritance_mode'] == COMPOUND_HET else cls._run_search
            num_results = run_search_func(
                search_name, config_search, family_variant_data, family_guid_map, dataset_type, sample_data,
                exclude=exclude, exclude_locations=exclude_locations, genes=search_genes, **config_search,
            )
            logger.info(f'Found {num_results} variants for criteria: {search_name}')
            search_counts[search_name] = num_results

    @classmethod
    def _get_valid_family_sample_data(cls, project, sample_type, samples_by_family, family_filter):
        if family_filter:
            samples_by_family = {
                family_guid: samples for family_guid, samples in samples_by_family.items()
                if cls._family_passes_filter(samples, family_filter)
            }
        return {
            'project_guids': [project.guid],
            'family_guids': samples_by_family.keys(),
            'sample_type_families': {sample_type: samples_by_family.keys()},
            'samples': [s for family_samples in samples_by_family.values() for s in family_samples],
        }

    @staticmethod
    def _family_passes_filter(samples, family_filter):
        affected = [s for s in samples if s['affected'] == Individual.AFFECTED_STATUS_AFFECTED]
        if family_filter.get('min_affected') and len(affected) < family_filter['min_affected']:
            return False
        if family_filter.get('max_affected') and len(affected) > family_filter['max_affected']:
            return False
        if 'confirmed_inheritance' in family_filter:
            proband = next((s for s in affected if s['maternal_guid'] and s['paternal_guid']), None)
            if not proband:
                return False
            loaded_guids = {s['individual_guid'] for s in samples}
            return proband['maternal_guid'] in loaded_guids and proband['paternal_guid'] in loaded_guids
        return True

    @staticmethod
    def _get_metadata(today, metadata_key):
        def wrapped(v):
            return {name: today for name in v[metadata_key]} if v[metadata_key] else None
        return wrapped

    @classmethod
    def _run_search(cls, search_name, config_search, family_variant_data, family_guid_map, dataset_type, sample_data, **kwargs):
        variant_fields = ['pos', 'end'] if dataset_type.startswith('SV') else ['ref', 'alt']
        variant_values = {'endChrom': F('end_chrom')} if dataset_type == 'SV_WGS' else {}
        results = [
            {**variant, 'genotypes': clickhouse_genotypes_json(variant['genotypes'])}
            for variant in gene_ids_annotated_queryset(get_search_queryset(
                GENOME_VERSION_GRCh38, dataset_type, sample_data, **kwargs,
            )).values(
                *variant_fields, 'key', 'xpos', 'familyGuids', 'genotypes', 'gene_ids',
                variantId=F('variant_id'), **variant_values,
            )
        ]
        require_mane_consequences = config_search.get('annotations', {}).get('vep_consequences')
        if require_mane_consequences:
            allowed_key_genes = cls._valid_mane_keys([v['key'] for v in results], require_mane_consequences)
            results = [r for r in results if r['key'] in allowed_key_genes]

        for variant in results:
            for family_guid in variant.pop('familyGuids'):
                variant_data = family_variant_data[(family_guid_map[family_guid], variant['variantId'])]
                variant_data.update(variant)
                variant_data['matched_searches'].add(search_name)

        return len(results)

    @classmethod
    def _run_comp_het_search(cls, search_name, config_search, family_variant_data, family_guid_map, dataset_type, sample_data, **kwargs):
        results = [v[1:] for v in get_data_type_comp_het_results_queryset(
            GENOME_VERSION_GRCh38, dataset_type, sample_data, **kwargs,
        )]

        primary_consequences = config_search.get('annotations', {}).get('vep_consequences')
        secondary_consequences = config_search.get('annotations_secondary', {}).get('vep_consequences')
        if primary_consequences or secondary_consequences:
            keys = [v['key'] for pair in results for v in pair]
            allowed_key_genes = cls._valid_mane_keys(keys, primary_consequences)
            if secondary_consequences:
                allowed_secondary_key_genes = cls._valid_mane_keys(keys, secondary_consequences)
            else:
                allowed_secondary_key_genes = None if config_search.get(
                    'no_secondary_annotations') else allowed_key_genes
            results = [
                pair for pair in results
                if allowed_key_genes.get(pair[0]['key']) == pair[0][SELECTED_GENE_FIELD] and (
                    allowed_secondary_key_genes is None or
                    allowed_secondary_key_genes.get(pair[1]['key']) ==pair[1][SELECTED_GENE_FIELD]
                )
            ]

        for pair in results:
            for family_guid in pair[0]['familyGuids']:
                for variant, support_id in [(pair[0], pair[1]['variantId']), (pair[1], pair[0]['variantId'])]:
                    variant_data = family_variant_data[(family_guid_map[family_guid], variant['variantId'])]
                    variant_data.update(variant)
                    variant_data['genotypes'] = clickhouse_genotypes_json(variant['genotypes'])
                    if 'transcripts' not in variant_data:
                        variant_data['gene_ids'] = list(dict.fromkeys([csq['geneId'] for csq in variant['sortedTranscriptConsequences']]))
                    variant_data['support_vars'].add(support_id)
                    variant_data['matched_comp_het_searches'].add(search_name)

        return len(results)

    @staticmethod
    def _valid_mane_keys(keys, allowed_consequences):
        mane_transcripts_by_key = get_transcripts_queryset(GENOME_VERSION_GRCh38, keys).values_list(
            'key', ArrayMap(
                ArrayFilter('transcripts', conditions=[{'maneSelect': (None, 'isNotNull({field})')}]),
                mapped_expression='tuple(x.consequenceTerms, x.geneId)',
                output_field=ArrayField(NamedTupleField([('consequenceTerms', ArrayField(StringField())), ('geneId', StringField())])),
            )
        )
        return {
            key: mane_transcripts[0]['geneId'] for key, mane_transcripts in mane_transcripts_by_key
            if mane_transcripts and set(allowed_consequences).intersection(mane_transcripts[0]['consequenceTerms'])
        }

    @staticmethod
    def _get_gene_list_genes(name, confidences, gene_by_moi, exclude_gene_ids):
        ll = LocusList.objects.get(name=name, palocuslist__isnull=False)
        moi_gene_ids = ll.locuslistgene_set.exclude(gene_id__in=exclude_gene_ids).annotate(
            is_dominant=Q(
                palocuslistgene__mode_of_inheritance__startswith='BOTH'
            ) | Q(
                palocuslistgene__mode_of_inheritance__startswith='X-LINKED',
                palocuslistgene__mode_of_inheritance__contains='monoallelic mutations',
            ) | Q(
                Q(palocuslistgene__mode_of_inheritance__startswith='MONOALLELIC') &
                ~Q(palocuslistgene__mode_of_inheritance__contains=' paternally imprinted') &
                ~Q(palocuslistgene__mode_of_inheritance__contains=' maternally imprinted')
            ),
            is_recessive=Q(
                palocuslistgene__mode_of_inheritance__startswith='BOTH'
            ) | Q(
                palocuslistgene__mode_of_inheritance__startswith='BIALLELIC'
            ) | Q(
                palocuslistgene__mode_of_inheritance__startswith='X-LINKED'
            ),
        ).filter(Q(is_dominant=True) | Q(is_recessive=True)).filter(palocuslistgene__confidence_level__in=[
            level for level, name in PaLocusListGene.CONFIDENCE_LEVEL_CHOICES if name in confidences
        ]).values('gene_id', 'is_dominant', 'is_recessive')

        dominant_gene_ids = [g['gene_id'] for g in moi_gene_ids if g['is_dominant']]
        recessive_gene_ids = [g['gene_id'] for g in moi_gene_ids if g['is_recessive']]
        genes_by_id = get_genes(dominant_gene_ids + dominant_gene_ids, genome_version=GENOME_VERSION_GRCh38, additional_model_fields=['id'])
        gene_by_moi['D'].update({gene_id: gene for gene_id, gene in genes_by_id.items() if gene_id in set(dominant_gene_ids)})
        gene_by_moi['R'].update({gene_id: gene for gene_id, gene in genes_by_id.items() if gene_id in set(recessive_gene_ids)})
