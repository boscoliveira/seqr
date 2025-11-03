from collections import defaultdict
from django.core.management.base import BaseCommand
from django.db.models import Q
import json
import os

from panelapp.models import PaLocusListGene
from reference_data.models import GENOME_VERSION_GRCh38
from seqr.models import Project, VariantTagType, LocusList
from seqr.utils.gene_utils import get_genes
from seqr.utils.search.utils import clickhouse_only
from seqr.views.utils.orm_to_json_utils import SEQR_TAG_TYPE

import logging
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('project')

    @clickhouse_only
    def handle(self, *args, **options):
        project = Project.objects.get(guid=options['project'])
        tag_type = VariantTagType.objects.get(name=SEQR_TAG_TYPE)
        with open(f'{os.path.dirname(__file__)}/../../fixtures/seqr_high_priority_searches.json', 'r') as file:
            config = json.load(file)

        exclude_genes = get_genes(config['exclude']['gene_ids'], genome_version=GENOME_VERSION_GRCh38)
        gene_by_moi = defaultdict(dict)
        for gene_list in config['geneLists']:
            self._get_gene_list_genes(gene_list['name'], gene_list['confidences'], gene_by_moi, exclude_genes.keys())

        for name, config_search in config['searches'].items():
            search = {**config_search, 'exclude': config['exclude']}
            if config_search.get('geneListMoi'):
                search['genes'] = gene_by_moi[config_search['geneListMoi']]
            else:
                search.update({'genes': exclude_genes, 'exclude_locations': True})

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
        genes_by_id = get_genes(dominant_gene_ids + dominant_gene_ids, genome_version=GENOME_VERSION_GRCh38)
        gene_by_moi['D'].update({gene_id: gene for gene_id, gene in genes_by_id.items() if gene_id in set(dominant_gene_ids)})
        gene_by_moi['R'].update({gene_id: gene for gene_id, gene in genes_by_id.items() if gene_id in set(recessive_gene_ids)})
