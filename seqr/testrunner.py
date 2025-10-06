# seqr/testrunner.py
from django.test.runner import DiscoverRunner


class OrderedDatabaseDeletionRunner(DiscoverRunner):

    # This class is necessary to resolve an issue with
    # clickhouse maintaining an open connection to the postgres
    # test database.  The "default" database is both created
    # and deleted first, but postgres (very reasonably) prevents a database
    # from being deleted if there exists an open connection.
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
