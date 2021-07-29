from models.serializer import SourceMapSerializer
from typing import Dict
from scrapy.selector.unified import Selector
from .base import BaseCollectionScraper, BaseContentExtraction
from scrapy.spiders import XMLFeedSpider
from scrapy.utils.spider import iterate_spider_output
from k11.models import SourceMap, Format, QueuedSourceMap
from scrapy.exceptions import NotConfigured, NotSupported



class CustomMarkupSpider(XMLFeedSpider, BaseCollectionScraper, BaseContentExtraction):
    
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

        itertype = None
        if "itertype" in kwargs:
            itertype = kwargs['itertype']

        if "format_rules" in kwargs and "itertag" in kwargs["format_rules"] and len(kwargs["format_rules"]["itertag"]) > 0:
            iterator = kwargs["format_rules"]["itertag"]
        if not hasattr(self, 'parse_node'):
            raise NotConfigured('You must define parse_node method in order to scrape this XML feed')

        response = self.adapt_response(response)
        if self.iterator == 'xml' or itertype == "xml":
            self.log(itertype, only_screen=True)
            selector = Selector(response, type='xml')
            self._register_namespaces(selector)
            selector.remove_namespaces()
            nodes = selector.xpath(f'//{iterator}')
        elif self.iterator == 'html' or itertype == "html":
            selector = Selector(response, type='html')
            self._register_namespaces(selector)
            nodes = selector.xpath(f'//{iterator}')
        else:
            selector = Selector(response)
            self._register_namespaces(selector)
            selector.remove_namespaces()
            nodes = selector.xpath(f'//{iterator}')
        return self.parse_nodes(response, nodes, **kwargs)




class CollectionSpider(CustomMarkupSpider):
    name = "feed_spider"
    itertag='item'

    handle_httpstatus_list = [404, 504]

    custom_settings = {

        "ITEM_PIPELINES": {
            "digger.pipelines.CollectionItemDuplicateFilterPipeline": 300,
            "digger.pipelines.CollectionItemSanitizingPipeline": 356,
            "digger.pipelines.CollectionItemVaultPipeline": 412
        },
        "LOG_LEVEL": "ERROR"
    }

    """
    Fetch all the sources from digger(db) and sources (collection) where is_third_party=False
    """
    def get_sources_from_database(self):
        if QueuedSourceMap.objects.count() > 0:
            return QueuedSourceMap.objects
        else:
            QueuedSourceMap.objects.insert([SourceMapSerializer(source_map) for source_map in SourceMap.objects])
        return QueuedSourceMap.objects
    
    """
    This method will return existing rss format attached with source, otherwise
    the default rss format

    """

    def _get_source_format_in_db(self, format_id: str) -> Format:
        try:
            return Format.objects(format_id=format_id).first()
        except Exception as e:
            return Format.objects.get_default_rss_format()
    

    """
    Find the format using format_id, in digger(db) and formats(collection) where source.source_id == formatter._id
    """

    def get_formatter_from_database(self, format_id: str) -> Format:
        return self._get_source_format_in_db(format_id)

    def before_requesting(self, **kwargs) -> Dict:
        if "format_rules" in kwargs and "itertag" in kwargs["format_rules"]:
            self.itertag = kwargs["format_rules"]["itertag"]
        return kwargs
    
    def error_handling(self, e):
        self.log(e)