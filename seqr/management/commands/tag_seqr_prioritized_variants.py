from collections import defaultdict
from datetime import datetime
from django.contrib.postgres.aggregates import ArrayAgg
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.db.models.functions import JSONObject
import json
import os

from clickhouse_search.search import get_search_queryset, SAMPLE_DATA_FIELDS
from panelapp.models import PaLocusListGene
from reference_data.models import GENOME_VERSION_GRCh38
from seqr.models import Project, LocusList, Sample
from seqr.utils.gene_utils import get_genes
from seqr.utils.search.utils import clickhouse_only, get_search_samples
from seqr.views.utils.orm_to_json_utils import SEQR_TAG_TYPE
from seqr.views.utils.variant_utils import bulk_create_tagged_variants

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
        sample_qs = get_search_samples([project]).filter(dataset_type=Sample.DATASET_TYPE_VARIANT_CALLS)
        sample_types = list(sample_qs.values_list('sample_type', flat=True).distinct())
        assert len(sample_types) == 1
        sample_type = sample_types[0]
        samples_by_family = dict(
            sample_qs.values('individual__family__guid').annotate(
                samples=ArrayAgg(JSONObject(**SAMPLE_DATA_FIELDS))).values_list('individual__family__guid', 'samples')
        )
        sample_data = {
            'project_guids': [project.guid],
            'family_guids': samples_by_family.keys(),
            'sample_type_families': {sample_type: samples_by_family.keys()},
            'samples': [s for family_samples in samples_by_family.values() for s in family_samples],
        }

        exclude_genes = get_genes(config['exclude']['gene_ids'], genome_version=GENOME_VERSION_GRCh38)
        gene_by_moi = defaultdict(dict)
        for gene_list in config['gene_lists']:
            self._get_gene_list_genes(gene_list['name'], gene_list['confidences'], gene_by_moi, exclude_genes.keys())

        family_variant_data = defaultdict(lambda: {'matched_searches': []})
        for search_name, config_search in config['searches'].items():
            exclude_locations = not config_search.get('gene_list_moi')
            search_genes = exclude_genes if exclude_locations else gene_by_moi[config_search['gene_list_moi']]
            results = get_search_queryset(
                GENOME_VERSION_GRCh38, Sample.DATASET_TYPE_VARIANT_CALLS, sample_data, **config_search,
                exclude=config['exclude'], exclude_locations=exclude_locations, genes=search_genes,
            ).values('key', 'xpos', 'variant_id', 'familyGuids', 'genotypes')
            for variant in results:
                for family_guid in variant.pop('familyGuids'):
                    family_variant_data[(family_guid, variant['variant_id'])].update(variant)
                    family_variant_data[(family_guid, variant['variant_id'])]['matched_searches'].append(search_name)

        today = datetime.now().strftime('%Y-%m-%d')
        num_new, num_updated = bulk_create_tagged_variants(
            family_variant_data, tag_name=SEQR_TAG_TYPE,
            get_metadata=lambda v: {name: today for name in v['matched_searches']}, user=None,
        )
        logger.info(f'Tagged {num_new} new and {num_updated} variants in {project.name}')
        # TODO family tag breakdown/ notifications


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
