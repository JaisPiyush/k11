
import os
from posixpath import dirname
from typing import Dict, List
from k11.models.main import ContentType, Format, LinkStore, SourceMap
import unittest
from k11.digger.spiders import RSSFeedSpider, HTMLFeedSpider
import requests
from scrapy.http import XmlResponse
from scrapy.selector import Selector




class TestRssFeedSpider(unittest.TestCase):
    spider_class = RSSFeedSpider

    def pull_named_source(self, source_name: str) -> SourceMap:
        return SourceMap.adapter().find_one({"source_name": source_name})
    
    def pull_rss_source_formatter(self, format_id: str) -> Format:
        return Format.adapter().find_one({"format_id": format_id})
    
    def null_test(self, output: Dict, format_rules):
        not_null_keys = ["title", "image", "link"]
        for key in not_null_keys:
            self.assertIsNotNone(output[key], f"{key} is null value")
    
    def output_test(self, format_rules, output: List[Dict]):
        for content, node in output:
            self.assertIsNotNone(node.namespaces, "Namespaces registration is not working")
            self.null_test(output=content, format_rules=format_rules)

    
    def test_collection_to_article(self):
        file_path = os.path.abspath(os.path.join(dirname(__file__), "fixtures/collection_to_article_xml.xml"))
        with open( file_path, "r") as file:
            response = XmlResponse(url="https://500px.com/editors.rss", body=file.read(),encoding='utf-8')
        format_rules = {
            "title": {
                "parent": "title",
                "param": "text()"
            },
            "images": {
                "is_cdata": True,
                "sel": "xpath",
                "parent": "description",
                "param": "/a//img/@src",
                "is_multiple": True
            }
        }
        expected_output = [{
            "title": "A Story About a Blue House by Rose Richards",
            "images": ["https://drscdn.500px.org/photo/1032439059/q%3D50_h%3D450/v2?sig=b529d7bcfb30f1a5619862afa351c8700ce86836040cee30f837530293d30368"]
        }, {
            "title": "Flyin? by Iza Łysoń",
            "images": ["https://drscdn.500px.org/photo/1032440486/q%3D50_h%3D450/v2?sig=802bbbc3668a2bb3c4e53166d2006d72f55b824429b92c7fdb78b4d086e72d5f"]
        }, {
            "title": "Missing Button by Nicola Pratt",
            "images": ["https://drscdn.500px.org/photo/1032442867/q%3D50_h%3D450/v2?sig=c4cc0949f9a26b86ccb5477fa1f953818dd30bee349674ced476ed9170af6dfe"]
        }]
        link_store = LinkStore(link="https://500px.com/editors.rss", content_type=ContentType.Image)
        source_map = SourceMap(source_name="500px", source_id="random", source_home_link="",assumed_tags="", 
                                compulsory_tags=[], is_collection=True, is_rss=True, links=[link_store])
        spider = self.spider_class()
        spider.itertag = "item"
        output = list(spider._parse(response, format_rules=format_rules, link_store=link_store, testing=True))
        self.assertEqual(len(output), 3)
        for index, data in enumerate(output):
            self.assertEqual(data[0]["title"], expected_output[index]["title"])
            self.assertEqual(data[0]["images"], expected_output[index]["images"])
            data[0]["link"] = "random_link"
            article_container = spider.process_single_article_data(data=data[0], link_store=link_store, source_map=source_map, index=index)
            self.assertEqual(article_container.article_link, "random_link")
            self.assertListEqual(article_container.images, expected_output[index]["images"])

            
    
    def test_rss_for_named_source(self):
        source_name = "Primer Magazine"
        source_map = self.pull_named_source(source_name)
        spider = self.spider_class()
        link_store = source_map.links[0]
        formats = spider.get_formatter_from_database(source_map.source_id)
        response = requests.get(link_store.link, headers={'User-Agent': "Mozilla/5.0 (Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"})
        response = XmlResponse(url=link_store.link, body=response.content)
        format_ =spider.get_suitable_format_rules(formats, source_map, link_store)
        kwarg = {
            "source_map": source_map,
            "format_rules": format_,
            "formats": formats,
            "assumed_tags": "", 
            "compulsory_tags": [],  
            "url": link_store.link,
            "link_store": link_store
        }
        spider.iterator = "xml"
        results1 = list(spider._parse(response, **kwarg))
        # print(format_)
        selector = Selector(response, type='xml')
        spider._register_namespaces(selector)
        selector.remove_namespaces()  
        nodes = selector.xpath(f"//{format_['itertag']}")
        self.assertEqual(type(response), XmlResponse)
        results2 = []
        for node in nodes:
            node = spider.parse_node(response, node, **kwarg)
            for gen in node:
                results2.append(gen)
        self.maxDiff = None
        self.assertListEqual(results1[0], results2[0])
    



class TestHTMLFeedSpider(unittest.TestCase):
    spider_class = HTMLFeedSpider
    