
from .base import BaseCollectionScraper, BaseContentExtraction

from scrapy.exceptions import NotConfigured, NotSupported
from scrapy.selector.unified import Selector
from k11.vault.exceptions import NoDocumentExists
from typing import Dict, Generator
from scrapy.spiders import XMLFeedSpider
from scrapy.utils.spider import iterate_spider_output
from k11.models.main import  SourceMap, Format


class XMLCustomImplmentation(XMLFeedSpider, BaseCollectionScraper, BaseContentExtraction):

    namespaces = (("dc","http://purl.org/dc/elements/1.1/"), ("media","http://search.yahoo.com/mrss/"), ("content", "http://purl.org/rss/1.0/modules/content/"), ("atom", "http://www.w3.org/2005/Atom"))

    

    def parse_node(self, response, selector, **kwargs):
        return self._parse_node(response, selector, **kwargs)

    def parse_nodes(self, response, nodes, **kwargs):
        """This method is called for the nodes matching the provided tag name
        (itertag). Receives the response and an Selector for each node.
        Overriding this method is mandatory. Otherwise, you spider won't work.
        This method must return either an item, a request, or a list
        containing any of them.
        """

        for index, selector in enumerate(nodes):
            kwargs["index"] = index
            ret = iterate_spider_output(self.parse_node(response, selector, **kwargs))
            for result_item in self.process_results(response, ret):
                    yield result_item


    def _parse(self, response, **kwargs):
        iterator = self.itertag
        if "format_rules" in kwargs and "itertag" in kwargs["format_rules"]:
            iterator = kwargs["format_rules"]["itertag"]
        if not hasattr(self, 'parse_node'):
            raise NotConfigured('You must define parse_node method in order to scrape this XML feed')

        response = self.adapt_response(response)
        if self.iterator == 'iternodes':
            nodes = self._iternodes(response)
        elif self.iterator == 'xml':
            selector = Selector(response, type='xml')
            self._register_namespaces(selector)
            selector.remove_namespaces()
            nodes = selector.xpath(f'//{iterator}')
        elif self.iterator == 'html':
            selector = Selector(response, type='html')
            self._register_namespaces(selector)
            nodes = selector.xpath(f'//{iterator}')
        else:
            raise NotSupported('Unsupported node iterator')
        
        return self.parse_nodes(response, nodes, **kwargs)







class RSSFeedSpider(XMLCustomImplmentation):
    name = "rss_feed_spider"
    itertag = 'item'
    iterator = 'xml'
    default_format_rules = "xml_collection_format"


    custom_settings = {

        "ITEM_PIPELINES": {
            "digger.pipelines.CollectionItemDuplicateFilterPiepline": 300,
            "digger.pipelines.CollectionItemSanitizingPipeline": 356,
            "digger.pipelines.CollectionItemVaultPipeline": 412
        }
    }


    """
    Fetch all the sources from digger(db) and sources (collection) where is_rss = True
    Insert all data into self.source_maps, which later will be used to iterate
    """
    def get_sources_from_database(self) -> Generator[SourceMap, None, None]:
        return SourceMap.pull_all_rss_models()

    """
    This method will return existing rss format attached with source, otherwise
    the default rss format

    """

    def _get_xml_source_format_in_db(self, format_id: str) -> Format:
        # print(format_id, "from get_xml_source")
        try:
            return Format.adapter().find_one({"format_id": format_id})
        except NoDocumentExists:
            return Format.get_default_rss_format()
    """
    Find the format using format_id, in digger(db) and formats(collection) where source.source_id == formatter._id
    """

    def get_formatter_from_database(self, format_id: str) -> Format:
        return self._get_xml_source_format_in_db(format_id)

    def before_requesting(self, **kwargs) -> Dict:
        if "format_rules" in kwargs and "itertag" in kwargs["format_rules"]:
            self.itertag = kwargs["format_rules"]["itertag"]
        return kwargs
    
    def error_handling(self, e):
        self.log(e)

   
