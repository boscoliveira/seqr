from django.core.management.base import BaseCommand
import json
import os

from seqr.models import Project, VariantTagType
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

        exclude_gene_ids = config['exclude']['gene_ids']
        for gene_list in config['geneLists']:
            pass
        print(len(config['searches']))
