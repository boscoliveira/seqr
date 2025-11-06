from collections import defaultdict
from datetime import datetime
from django.contrib.postgres.aggregates import ArrayAgg
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.db.models.functions import JSONObject
import json
import os

from clickhouse_backend.models import ArrayField, StringField
from clickhouse_search.backend.functions import ArrayFilter, ArrayMap
from clickhouse_search.search import get_search_queryset, get_transcripts_queryset, clickhouse_genotypes_json, \
    SAMPLE_DATA_FIELDS
from panelapp.models import PaLocusListGene
from reference_data.models import GENOME_VERSION_GRCh38
from seqr.models import Project, Family, Sample, LocusList, Individual
from seqr.utils.communication_utils import send_project_notification
from seqr.utils.gene_utils import get_genes
from seqr.utils.search.utils import clickhouse_only, get_search_samples
from seqr.views.utils.orm_to_json_utils import SEQR_TAG_TYPE
from seqr.views.utils.variant_utils import bulk_create_tagged_variants, VARIANT_GENE_IDS_EXPRESSION
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

        project = Project.objects.get(guid=options['project'])
        # TODO SVs
        sample_qs = get_search_samples([project]).filter(dataset_type=Sample.DATASET_TYPE_VARIANT_CALLS)
        sample_types = list(sample_qs.values_list('sample_type', flat=True).distinct())
        assert len(sample_types) == 1
        sample_type = sample_types[0]
        samples_by_family = {
            family_guid: samples for family_guid, samples in sample_qs.values('individual__family__guid').annotate(
                samples=ArrayAgg(JSONObject(**SAMPLE_DATA_FIELDS))).values_list('individual__family__guid', 'samples')
            if any(s['affected'] == Individual.AFFECTED_STATUS_AFFECTED for s in samples)
        }

        exclude_genes = get_genes(config['exclude']['gene_ids'], genome_version=GENOME_VERSION_GRCh38)
        gene_by_moi = defaultdict(dict)
        for gene_list in config['gene_lists']:
            self._get_gene_list_genes(gene_list['name'], gene_list['confidences'], gene_by_moi, exclude_genes.keys())

        family_guid_map = {}
        family_name_map = {}
        for db_id, guid, family_id in Family.objects.filter(project=project).values_list('id', 'guid', 'family_id'):
            family_guid_map[guid] = db_id
            family_name_map[db_id] = family_id

        logger.info(f'Searching for prioritized variants in {len(samples_by_family)} families in project {project.name}')
        family_variant_data = defaultdict(lambda: {'matched_searches': []})
        search_counts = {}
        for search_name, config_search in config['searches'].items():
            exclude_locations = not config_search.get('gene_list_moi')
            search_genes = exclude_genes if exclude_locations else gene_by_moi[config_search['gene_list_moi']]
            sample_data = self._get_valid_family_sample_data(
                project, sample_type, samples_by_family, config_search.get('family_filter'),
            )
            results = get_search_queryset(
                GENOME_VERSION_GRCh38, Sample.DATASET_TYPE_VARIANT_CALLS, sample_data, **config_search,
                exclude=config['exclude'], exclude_locations=exclude_locations, genes=search_genes,
            ).values('key', 'xpos', 'ref', 'alt', 'variant_id', 'familyGuids', 'genotypes', gene_ids=VARIANT_GENE_IDS_EXPRESSION)
            if config_search.get('annotations'):
                results = self._filter_mane_transcript(results, config_search['annotations'])
            search_counts[search_name] = len(results)
            logger.info(f'Found {len(results)} variants matching criteria "{search_name}"')
            for variant in results:
                for family_guid in variant.pop('familyGuids'):
                    variant_data = family_variant_data[(family_guid_map[family_guid], variant['variant_id'])]
                    variant_data.update({**variant, 'genotypes': clickhouse_genotypes_json(variant['genotypes'])})
                    variant_data['matched_searches'].append(search_name)

        today = datetime.now().strftime('%Y-%m-%d')
        new_tag_keys, num_updated, num_skipped = bulk_create_tagged_variants(
            family_variant_data, tag_name=SEQR_TAG_TYPE, get_metadata=lambda v: {name: today for name in v['matched_searches']},
            user=None, remove_missing_metadata=False,
        )

        family_variants = defaultdict(list)
        for family_id, variant_id in family_variant_data.keys():
            family_variants[family_id].append(variant_id)
        logger.info(f'Tagged {len(new_tag_keys)} new and {num_updated} previously tagged variants in {len(family_variants)} families, found {num_skipped} unchanged tags:')
        for search_name, count in search_counts.items():
            logger.info(f'  {search_name}: {count} variants')
        if not new_tag_keys:
            return

        send_project_notification(
            project,
            notification=f'{len(new_tag_keys)} new seqr prioritized variants',
            subject='New prioritized variants tagged in seqr',
            email_template='This is to notify you that {notification} have been tagged in seqr project {project_link}',
            slack_channel=SEQR_SLACK_DATA_ALERTS_NOTIFICATION_CHANNEL,
            slack_detail='\n'.join(sorted([
                f'{family_name_map[family_id]}: {len(variants)} new tags' for family_id, variants in family_variants.items()
            ])),
        )

    @staticmethod
    def _get_valid_family_sample_data(project, sample_type, samples_by_family, family_filter):
        if family_filter:
            min_affected = family_filter.get('min_affected', 1)
            samples_by_family = {
                family_guid: samples for family_guid, samples in samples_by_family.items()
                if len([s for s in samples if s['affected'] == Individual.AFFECTED_STATUS_AFFECTED]) >= min_affected
            }
        return {
            'project_guids': [project.guid],
            'family_guids': samples_by_family.keys(),
            'sample_type_families': {sample_type: samples_by_family.keys()},
            'samples': [s for family_samples in samples_by_family.values() for s in family_samples],
        }

    @staticmethod
    def _filter_mane_transcript(results, annotations):
        mane_csqs_by_key = dict(get_transcripts_queryset(GENOME_VERSION_GRCh38, [v['key'] for v in results]).values_list(
            'key', ArrayMap(
                ArrayFilter('transcripts', conditions=[{'maneSelect': (None, 'isNotNull({field})')}]),
                mapped_expression='x.consequenceTerms',
                output_field=ArrayField(StringField()),
            )
        ))
        allowed_csqs = set(annotations['vep_consequences'])
        return [r for r in results if mane_csqs_by_key[r['key']] and allowed_csqs.intersection(mane_csqs_by_key[r['key']][0])]

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
