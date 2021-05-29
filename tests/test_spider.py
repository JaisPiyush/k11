
from typing import Dict, List
from k11.models.main import Format, SourceMap
import unittest
from k11.digger.spiders import RSSFeedSpider
import requests




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
            # self.assertListEqual(list(self.spider_class.namespaces), list(node.namespaces), list(node.namespaces))
            print(content)
            self.null_test(output=content, format_rules=format_rules)



    
    def test_rss_for_named_source(self):
        source_name = "iso_500px"
        source_map = self.pull_named_source(source_name)
        formatter = self.pull_rss_source_formatter(source_map.source_id)
        spider = self.spider_class()
        spider.itertag = "item"
        for link_store in source_map.links:
            print("Scrapping link "+link_store.link)
            format_rules = spider.get_suitable_format_rules(formats=formatter, source=source_map, link_store=link_store, default='xml_collection_format')
            response = requests.get(link_store.link)
            output = list(spider._parse(response=response.content, format_rules=format_rules, testing=True))
            node = output[0][-1]
            print(f'//{format_rules["title"]["parent"]}/{format_rules["title"]["param"]}')
            print("title", node.xpath('//title'))
            self.output_test( format_rules=format_rules, output=output)



