from django.core.management import call_command
import mock

from clickhouse_search.search_tests import ClickhouseSearchTestCase

PROJECT_GUID = 'R0001_1kg'


class CheckNewSamplesTest(ClickhouseSearchTestCase):
    databases = '__all__'
    fixtures = ['users', '1kg_project', 'reference_data', 'clickhouse_transcripts']

    @mock.patch('seqr.utils.search.add_data_utils.SEQR_SLACK_DATA_ALERTS_NOTIFICATION_CHANNEL', 'seqr-data-loading')
    @mock.patch('seqr.utils.communication_utils.EmailMultiAlternatives')
    def test_command(self, mock_email):
        call_command('tag_seqr_prioritized_variants', PROJECT_GUID)
        self.assert_json_logs(user=None, expected=[('Hello World', None)])
