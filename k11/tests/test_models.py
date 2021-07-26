from k11.models.sql_models import IndexableLinks
from k11.models.no_sql_models import Format, SourceMap
from k11.vault.app import connection_handler
from unittest import TestCase
from sqlalchemy import func
from sqlalchemy.sql.expression import distinct
from sqlalchemy.sql.operators import exists


class TestNoSqlWorking(TestCase):
    def setUp(self) -> None:
        connection_handler.mount_mongo_engines()
    
    def test_source_map(self):
        counts = SourceMap.objects.count()
        self.assertGreaterEqual(counts, 10)
        format_ = Format.objects(format_id="WSTvVDLbD-5nbmqOxtkX3A_scoop_woop").get()
        self.assertNotEqual(format_, None)
    
    def tearDown(self) -> None:
        connection_handler.disconnect_mongo_engines()

