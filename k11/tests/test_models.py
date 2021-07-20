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


class TestSqlWorking(TestCase):
    def setUp(self) -> None:
        connection_handler.mount_sql_engines()
        self.session = connection_handler.create_sql_session()
        print(self.session)
    
    def test_indexable_links(self):
        query = self.session.query(func.count(distinct(IndexableLinks.link)))
        counts = self.session.execute(query).scalar()
        self.assertGreater(counts, 10)

        query = self.session.query(IndexableLinks).filter(IndexableLinks.link == 'abs')
        exist = self.session.query(query.exists()).scalar()
        self.assertEqual(exist, False)
    
    def tearDown(self) -> None:
        connection_handler.dispose_sql_engines(self.session)
