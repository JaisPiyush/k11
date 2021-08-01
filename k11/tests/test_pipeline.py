import re
from unittest import TestCase
from k11.digger.spiders import ArticleSpider
from k11.digger.pipelines import ArticlePreprocessor
from scrapy.http import HtmlResponse
from k11.vault import connection_handler
from scrapy import Selector
import os
from posixpath import dirname



class TestArticleScrapper(TestCase):
    spider_cls = ArticleSpider
    sanitizer = ArticlePreprocessor()

    def setUp(self) -> None:
        connection_handler.mount_all_engines()
    
    def test_extraction_processing(self):
        data = {'images': ['https://images.squarespace-cdn.com/content/v1/5c0a0a92b27e3986f51e5eb0/1611795760790-F9IJD5BWGH6WRXNDQ0Z0/1H4A8513.jpg?format=300w']}
        ps = {
            "images": {
                "op": "replace",
                "params": {
                    "pattern": re.compile("(\?format=\d+w)+"),
                    "repl": "",
                    "string": "self"
                }
            }
        }
        data_ = self.sanitizer.process_extracted_data(data, ps)
        self.assertEqual(data_["images"][0], 'https://images.squarespace-cdn.com/content/v1/5c0a0a92b27e3986f51e5eb0/1611795760790-F9IJD5BWGH6WRXNDQ0Z0/1H4A8513.jpg')

    
    def test_article_spider(self):
        links = []


    
    def tearDown(self) -> None:
        connection_handler.disconnect_mongo_engines()
        connection_handler.dispose_sql_engines()