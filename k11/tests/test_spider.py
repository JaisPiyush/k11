
import os
from posixpath import dirname
from typing import Dict, List
from k11.models import ContentType, Format, LinkStore, SourceMap
from unittest import TestCase
from k11.digger.spiders.collection_spider import CollectionSpider
from k11.vault import connection_handler
from scrapy.http import XmlResponse, HtmlResponse
from scrapy.selector import Selector




class TestCollectionSpider(TestCase):
    spider_class = CollectionSpider


    def setUp(self) -> None:
        connection_handler.mount_mongo_engines()
    
    def tearDown(self) -> None:
        connection_handler.disconnect_mongo_engines()
    

    def pull_named_source(self, source_name: str) -> SourceMap:
        return SourceMap.objects(source_name = source_name).get()
    
    def pull_rss_source_formatter(self, format_id: str) -> Format:
        return Format.objects(format_id=format_id).get()
    
    def null_test(self, output: Dict, format_rules):
        not_null_keys = ["title", "image", "link"]
        for key in not_null_keys:
            self.assertIsNotNone(output[key], f"{key} is null value")
    
    def output_test(self, format_rules, output: List[Dict]):
        for content, node in output:
            self.assertIsNotNone(node.namespaces, "Namespaces registration is not working")
            self.null_test(output=content, format_rules=format_rules)
    

    def test_xml_scrapper(self):
        file_path = os.path.abspath(os.path.join(dirname(__file__), "fixtures/xml_scrapper.xml"))
        with open(file_path, "r") as file:
            response = XmlResponse("https://www.pinkvilla.com/category/entertainment/news/feed", body=file.read(), encoding='utf-8')
        format_rules = {
        "itertag": "item",
        "title": {
            "sel": "xpath",
            "param": "text()",
            "parent": "title",
            "type": "text",
            "is_multiple": False,
            "is_cdata": False
        },
        "link": {
            "sel": "xpath",
            "param": "text()",
            "parent": "link",
            "type": "text",
            "is_multiple": False,
            "is_cdata": False
        },
        "creator": {
            "sel": "xpath",
            "param": "text()",
            "parent": "creator",
            "type": "text",
            "is_multiple": False,
            "is_cdata": True
        },
        }

        expected_outputs = [{
            "title": "Raj Kundra Pornography Case: Evidence against accomplice involved in distribution of 90 obscene clips procured",
            "link": "http://www.pinkvilla.com/entertainment/news/raj-kundra-pornography-case-evidence-against-accomplice-involved-distribution-90-obscene-clips-procured-830617",
            "creator": None
        }, {
            "title": "Kriti Kharbanda on wedding plans with Pulkit Samrat: These are the things meant only for me and my family",
            "link": "http://www.pinkvilla.com/entertainment/news/kriti-kharbanda-wedding-plans-pulkit-samrat-these-are-things-meant-only-me-and-my-family-830614",
            "creator": None
        }]

        link_store = LinkStore(link="https://www.pinkvilla.com/category/entertainment/news/feed", content_type=ContentType.Article, formatter="xml_collection_format")
        source_map = SourceMap(source_name="500px", source_id="random", source_home_link="",assumed_tags="", 
                                compulsory_tags=[], is_collection=True, is_rss=True, links=[link_store])
        spider = self.spider_class()
        outputs = list(spider._parse(response, format_rules=format_rules, link_store=link_store, source_map=source_map, testing=True, itertype="xml"))
        self.assertGreater(len(outputs), 0)

        contains_outputs = []
        for output, node in outputs:
            print(output)
            if output in expected_outputs:
                contains_outputs.append(True)
        # print(outputs)
        self.assertEqual(len(expected_outputs), len(contains_outputs))
    


    def test_html_scrapper(self):
        file_path = os.path.abspath(os.path.join(dirname(__file__), "fixtures/html_scrapper.html"))
        with open(file_path, "r") as file:
            response = HtmlResponse("https://www.pinkvilla.com/entertainment/movie-review", body=file.read(), encoding='utf-8')
        format_rules = {
        "itertag": "div[@class=\"article-page-teaser\"]",
        "title": {
            "sel": "xpath",
            "param": "text()",
            "parent": "a[@class=\"section-title\"]",
            "type": "text",
            "is_multiple": False,
            "is_cdata": False
        },
        "link": {
            "sel": "xpath",
            "param": "@href",
            "parent": "a",
            "type": "text",
            "is_multiple": False,
            "is_cdata": False
        },
        "creator": {
            "sel": "xpath",
            "param": "text()",
            "parent": "creator",
            "type": "text",
            "is_multiple": False,
            "is_cdata": False
        },
        # "image": {
        #     "sel": "xpath",
        #     "param": "@data-src",
        #     "parent": "img",
        #     "type": "image",
        #     "is_multiple": False,
        #     "is_cdata": False
        # }
    }

        expected_outputs = [{
            "title": "Ek Duaa Review: Esha Deol's short film on female foeticide is a mediocre take on a significant issue ",
            "link": "https://www.pinkvilla.com/entertainment/reviews/ek-duaa-review-esha-deols-short-film-female-foeticide-mediocre-take-significant-issue-829560",
            "creator": None
        }, {
            "title": "Hungama 2 Review: Priyadarshan's comedy with Shilpa Shetty, Meezaan & Paresh Rawal is too long to be funny",
            "link": "https://www.pinkvilla.com/entertainment/reviews/hungama-2-review-priyadarshans-comedy-shilpa-shetty-meezaan-paresh-rawal-too-long-be-funny-827089",
            "creator": None
        }]

        link_store = LinkStore(link="https://www.pinkvilla.com/entertainment/movie-review", content_type=ContentType.Article, formatter="html_collection_format")
        source_map = SourceMap(source_name="500px", source_id="random", source_home_link="",assumed_tags="", 
                                compulsory_tags=[], is_collection=True, is_rss=True, links=[link_store])
        spider = self.spider_class()
        outputs = list(spider._parse(response, format_rules=format_rules, link_store=link_store, source_map=source_map, testing=True, itertype="html"))
        self.assertGreater(len(outputs), 0)

        contains_outputs = []
        for output, node in outputs:
            print(output)
            if output in expected_outputs:
                contains_outputs.append(True)
        # print(outputs)
        self.assertEqual(len(expected_outputs), len(contains_outputs))





    
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
        output = list(spider._parse(response, format_rules=format_rules, link_store=link_store, testing=True, itertype='xml'))
        self.assertEqual(len(output), 3)
        for index, data in enumerate(output):
            self.assertEqual(data[0]["title"], expected_output[index]["title"])
            self.assertEqual(data[0]["images"], expected_output[index]["images"])
            data[0]["link"] = "random_link"
            article_container = spider.process_single_article_data(data=data[0], link_store=link_store, source_map=source_map, index=index)
            self.assertEqual(article_container.article_link, "random_link")
            self.assertListEqual(article_container.images, expected_output[index]["images"])

            


    