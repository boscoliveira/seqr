from datetime import datetime
from django.core.management import call_command
import mock

from clickhouse_search.search_tests import ClickhouseSearchTestCase
from clickhouse_search.test_utils import VARIANT2, VARIANT3, VARIANT4, GCNV_VARIANT3, GCNV_VARIANT4
from seqr.models import SavedVariant, VariantTag

PROJECT_GUID = 'R0001_1kg'

SNV_INDEL_MATCHES = {
    'Clinvar Pathogenic': 0,
    'Clinvar Pathogenic -  Compound Heterozygous': 0,
    'Clinvar Pathogenic - Recessive': 1,
    'Compound Heterozygous': 1,
    'Compound Heterozygous - Confirmed': 0,
    'De Novo': 0,
    'De Novo/ Dominant': 0,
    'Dominant': 0,
    'High Splice AI': 0,
    'Recessive': 1,
}
SV_MATCHES = {
    'SV - Compound Heterozygous': 1,
    'SV - De Novo/ Dominant': 0,
    'SV - Recessive': 1,
}
MULTI_TYPE_MATCHES = {
    'Compound Heterozygous - One SV': 1,
}

class CheckNewSamplesTest(ClickhouseSearchTestCase):
    databases = '__all__'
    fixtures = ['users', '1kg_project', 'reference_data', 'panelapp', 'clickhouse_transcripts']

    @mock.patch('seqr.utils.communication_utils.EmailMultiAlternatives')
    @mock.patch('seqr.utils.communication_utils._post_to_slack')
    @mock.patch('seqr.management.commands.tag_seqr_prioritized_variants.datetime')
    def test_command(self, mock_datetime, mock_slack, mock_email):
        mock_datetime.now.return_value = datetime(2025, 11, 15)

        call_command('tag_seqr_prioritized_variants', PROJECT_GUID)
        self.assert_json_logs(user=None, expected=[
            ('Searching for prioritized SNV_INDEL variants in 3 families in project 1kg project n\u00e5me with uni\u00e7\u00f8de', None),
        ] + [(f'Found {count} variants for criteria: {criteria}', None) for criteria, count in SNV_INDEL_MATCHES.items()] + [
            ('Searching for prioritized SV_WES variants in 1 families in project 1kg project n\u00e5me with uni\u00e7\u00f8de', None),
        ] + [(f'Found {count} variants for criteria: {criteria}', None) for criteria, count in SV_MATCHES.items()] + [
            ('Searching for prioritized multi data type variants in 1 families in project 1kg project n\u00e5me with uni\u00e7\u00f8de', None),
        ] + [(f'Found {count} variants for criteria: {criteria}', None) for criteria, count in MULTI_TYPE_MATCHES.items()] + [
            ('create 5 SavedVariants', {
                'dbUpdate': {'dbEntity': 'SavedVariant', 'entityIds': mock.ANY, 'updateType': 'bulk_create'},
            }),
        ] + [
            (f'create VariantTag VT{db_id}_seqr_prioritized', {'dbUpdate': {
                'dbEntity': 'VariantTag', 'entityId': f'VT{db_id}_seqr_prioritized', 'updateFields': ['metadata', 'variant_tag_type'], 'updateType': 'create',
            }}) for db_id in range(1726986, 1726991)
        ] + [
            ('Tagged 5 new and 0 previously tagged variants in 1 families, found 0 unchanged tags:', None),
        ] + [(f'  {criteria}: {count} variants', None) for criteria, count in  SNV_INDEL_MATCHES.items()] + [
            (f'  {criteria}: {count} variants', None) for criteria, count in  SV_MATCHES.items()
        ] + [(f'  {criteria}: {count} variants', None) for criteria, count in  MULTI_TYPE_MATCHES.items()])

        new_saved_variants = SavedVariant.objects.filter(key__in=[2, 3, 4, 18, 19]).order_by('key').values(
            'key', 'variant_id', 'family_id', 'dataset_type', 'xpos', 'xpos_end', 'ref', 'alt', 'gene_ids', 'genotypes', 'saved_variant_json',
        )
        self.assertListEqual(list(new_saved_variants),  [{
            'key': 2, 'variant_id': '1-38724419-T-G', 'family_id': 2, 'dataset_type': 'SNV_INDEL', 'xpos': 1038724419,
            'xpos_end': 1038724419, 'ref': 'T', 'alt': 'G', 'gene_ids': ['ENSG00000177000', 'ENSG00000277258'],
            'genotypes': VARIANT2['genotypes'], 'saved_variant_json': {},
        }, {'key': 3, 'variant_id': '1-91502721-G-A', 'family_id': 2, 'dataset_type': 'SNV_INDEL', 'xpos': 1091502721,
            'xpos_end': 1091502721, 'ref': 'G', 'alt': 'A', 'gene_ids': ['ENSG00000097046', 'ENSG00000177000'],
            'genotypes': VARIANT3['genotypes'], 'saved_variant_json': {},
        }, {'key': 4, 'variant_id': '1-91511686-T-G', 'family_id': 2, 'dataset_type': 'SNV_INDEL', 'xpos': 1091511686,
            'xpos_end': 1091511686, 'ref': 'T', 'alt': 'G', 'gene_ids': ['ENSG00000097046'],
            'genotypes': VARIANT4['genotypes'], 'saved_variant_json': {},
        }, {'key': 18, 'variant_id': 'suffix_140593_DUP', 'family_id': 2, 'dataset_type': 'SV_WES', 'xpos': 17038717327,
            'xpos_end': 17038719993, 'ref': None, 'alt': None, 'gene_ids': ['ENSG00000275023'],
            'genotypes': GCNV_VARIANT3['genotypes'], 'saved_variant_json': {},
        }, {'key': 19, 'variant_id': 'suffix_140608_DUP', 'family_id': 2, 'dataset_type': 'SV_WES', 'xpos': 17038721781,
            'xpos_end': 17038735703, 'ref': None, 'alt': None, 'gene_ids': ['ENSG00000275023', 'ENSG00000277258', 'ENSG00000277972'],
            'genotypes': GCNV_VARIANT4['genotypes'], 'saved_variant_json': {},
        }])

        tags = VariantTag.objects.filter(variant_tag_type__name='seqr Prioritized').order_by('id')
        self.assertListEqual(list(tags.values_list('metadata', flat=True)), [
            '{"Clinvar Pathogenic - Recessive": "2025-11-15", "Recessive": "2025-11-15"}',
            '{"Compound Heterozygous - One SV": "2025-11-15"}',
            '{"Compound Heterozygous": "2025-11-15"}',
            '{"SV - Recessive": "2025-11-15"}',
            '{"SV - Compound Heterozygous": "2025-11-15"}',
        ])
        self.assertListEqual([list(tag.saved_variants.values_list('key', flat=True)) for tag in tags], [
            [2], [2, 19], [3, 4], [18], [18, 19],
        ])

        # Test notifications

        # Test no new variants to tag
