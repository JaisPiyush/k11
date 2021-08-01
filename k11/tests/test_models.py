from k11.digger.spiders.collection_spider import CollectionSpider
from k11.models.no_sql_models import Format, SourceMap
from k11.vault.app import connection_handler
from unittest import TestCase

from mongoengine.queryset.visitor import Q


class TestNoSqlWorking(TestCase):
    def setUp(self) -> None:
        connection_handler.mount_mongo_engines()
    
    def test_source_map(self):
        counts = SourceMap.objects.count()
        self.assertGreaterEqual(counts, 10)
        format_ = Format.objects(format_id="WSTvVDLbD-5nbmqOxtkX3A_scoop_woop").get()
        self.assertNotEqual(format_, None)
    
    def test_queued_source_map(self):
        queryset = CollectionSpider.get_sources_from_database()
        source_maps = SourceMap.objects(Q(is_collection=True) & Q(is_third_party=False))
        self.assertEqual(queryset.count(), source_maps.count())
    
    def tearDown(self) -> None:
        connection_handler.disconnect_mongo_engines()

