# seqr/testrunner.py
from django.test.runner import DiscoverRunner


class OrderedDatabaseDeletionRunner(DiscoverRunner):

    def teardown_databases(self, old_config, **kwargs):
        clickhouse_dbs = []
        postgres_dbs = []

        for conn, old_name, destroy in old_config:
            if 'clickhouse' in conn.alias:
                clickhouse_dbs.append((conn, old_name, destroy))
            else:
                postgres_dbs.append((conn, old_name, destroy))

        super().teardown_databases(clickhouse_dbs, **kwargs)
        super().teardown_databases(postgres_dbs, **kwargs)