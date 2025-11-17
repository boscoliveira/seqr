from django.core.management import call_command
import mock

from clickhouse_search.search_tests import ClickhouseSearchTestCase

PROJECT_GUID = 'R0001_1kg'

SNV_INDEL_MATCHES = {
    'Clinvar Pathogenic': 0,
    'Clinvar Pathogenic -  Compound Heterozygous': 0,
    'Clinvar Pathogenic - Recessive': 0,
    'Compound Heterozygous': 0,
    'Compound Heterozygous - Confirmed': 0,
    'De Novo': 0,
    'De Novo/ Dominant': 0,
    'Dominant': 0,
    'High Splice AI': 0,
    'Recessive': 0,
}
SV_MATCHES = {
    'SV - Compound Heterozygous': 1,
    'SV - De Novo/ Dominant': 0,
    'SV - Recessive': 1,
}
MULTI_TYPE_MATCHES = {
    'Compound Heterozygous - One SV': 0,
}

class CheckNewSamplesTest(ClickhouseSearchTestCase):
    databases = '__all__'
    fixtures = ['users', '1kg_project', 'reference_data', 'clickhouse_transcripts']

    @mock.patch('seqr.utils.communication_utils.EmailMultiAlternatives')
    @mock.patch('seqr.utils.communication_utils._post_to_slack')
    def test_command(self, mock_slack, mock_email):
        call_command('tag_seqr_prioritized_variants', PROJECT_GUID)
        self.assert_json_logs(user=None, expected=[
            ('Searching for prioritized SNV_INDEL variants in 3 families in project 1kg project n\u00e5me with uni\u00e7\u00f8de', None),
        ] + [(f'Found {count} variants for criteria: {criteria}', None) for criteria, count in SNV_INDEL_MATCHES.items()] + [
            ('Searching for prioritized SV_WES variants in 1 families in project 1kg project n\u00e5me with uni\u00e7\u00f8de', None),
        ] + [(f'Found {count} variants for criteria: {criteria}', None) for criteria, count in SV_MATCHES.items()] + [
            ('Searching for prioritized multi data type variants in 1 families in project 1kg project n\u00e5me with uni\u00e7\u00f8de', None),
        ] + [(f'Found {count} variants for criteria: {criteria}', None) for criteria, count in MULTI_TYPE_MATCHES.items()] + [
            ('create 2 SavedVariants', {
                'dbUpdate': {'dbEntity': 'SavedVariant', 'entityIds': mock.ANY, 'updateType': 'bulk_create'},
            }),
        ] + [
            (f'create VariantTag VT{db_id}_seqr_prioritized', {'dbUpdate': {
                'dbEntity': 'VariantTag', 'entityId': f'VT{db_id}_seqr_prioritized', 'updateFields': ['metadata', 'variant_tag_type'], 'updateType': 'create',
            }}) for db_id in range(1726986, 1726988)
        ] + [
            ('Tagged 2 new and 0 previously tagged variants in 1 families, found 0 unchanged tags:', None),
        ] + [(f'  {criteria}: {count} variants', None) for criteria, count in  SNV_INDEL_MATCHES.items()] + [
            (f'  {criteria}: {count} variants', None) for criteria, count in  SV_MATCHES.items()
        ] + [(f'  {criteria}: {count} variants', None) for criteria, count in  MULTI_TYPE_MATCHES.items()])
