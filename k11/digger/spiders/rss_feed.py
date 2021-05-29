
from .base import BaseCollectionScraper, MultiplContentFromCollection

from scrapy.exceptions import NotConfigured, NotSupported
from scrapy.selector.unified import Selector
from k11.utils import is_url_valid
from k11.vault.exceptions import NoDocumentExists
from typing import Dict, Generator, List, Union
from scrapy.spiders import XMLFeedSpider
from scrapy.utils.spider import iterate_spider_output
from k11.models.main import  SourceMap, Format


class XMLCustomImplmentation(XMLFeedSpider, BaseCollectionScraper, MultiplContentFromCollection):

    namespaces = (("dc","http://purl.org/dc/elements/1.1/"), ("media","http://search.yahoo.com/mrss/"), ("content", "http://purl.org/rss/1.0/modules/content/"), ("atom", "http://www.w3.org/2005/Atom"))

    

    def parse_node(self, response, selector, **kwargs):
        """This method must be overriden with your custom spider functionality"""
        if hasattr(self, 'parse_item'):  # backward compatibility
            return self.parse_item(response, selector, **kwargs)
        raise NotImplementedError

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
        if not hasattr(self, 'parse_node'):
            raise NotConfigured('You must define parse_node method in order to scrape this XML feed')

        response = self.adapt_response(response)
        if self.iterator == 'iternodes':
            nodes = self._iternodes(response)
        elif self.iterator == 'xml':
            selector = Selector(response, type='xml')
            self._register_namespaces(selector)
            # selector.remove_namespaces()
            nodes = selector.xpath(f'//{self.itertag}')
        elif self.iterator == 'html':
            selector = Selector(response, type='html')
            self._register_namespaces(selector)
            nodes = selector.xpath(f'//{self.itertag}')
        else:
            raise NotSupported('Unsupported node iterator')
        
        return self.parse_nodes(response, nodes, **kwargs)







class RSSFeedSpider(XMLCustomImplmentation):
    name = "rss_feed_spider"
    itertag = 'item'
    non_formattable_tags = ['itertag', 'namespaces']


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

    def pull_rss_sources_from_db(self) -> Generator[SourceMap, None, None]:
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

    def pull_rss_source_formatters(self, format_id: str) -> Format:
        return self._get_xml_source_format_in_db(format_id)

  

    """
    Every Source Map contains links, which are LinkStore containing link and optionaly assumed_tags, and other params.
    If any link store contains their personal formatter, than the formatter will be passed into parser,
    else the default fomatter of source map will be passed
    """

    def start_requests(self):
        for source in self.pull_rss_sources_from_db():
            formats = self.pull_rss_source_formatters(
                source.source_id)
            if formats == None:
                continue
            for link_store in source.links:
                if link_store.link != None and len(link_store.link) > 0 and is_url_valid(link_store.link):
                    format_rules = self.get_suitable_format_rules(formats=formats, source=source, link_store=link_store, default="xml_collection_format")
                    assumed_tags, compulsory_tags = self.get_tags(source=source, link_store=link_store)
                    if "itertag" in format_rules:
                        self.itertag = format_rules["itertag"]
                    yield self.call_request(url=link_store.link, callback=self._parse, formats=formats, format_rules=format_rules, source=source, 
                    assumed_tags=assumed_tags, compulsory_tags=compulsory_tags, link_store=link_store)
                else:
                    continue
    
    def parse_cdata(self, node: Selector, query: Dict):
        cdata_text = self.extract_values(node, parent=query["parent"], param='text()', sel="xpath")
        selected = Selector(text=cdata_text)
        query_copy = query.copy()
        del query_copy["param"]
        del query_copy["parent"]
        return self.extract_values(node=selected, parent=query["param"], param='', param_prefix='', parent_prefix='./', **query_copy)

    
    def extract_values(self, node: Selector, parent: str, param: str="text()", parent_prefix=".//", param_prefix="/", **kwargs) -> Union[str, List[str]]:
        f_str = parent_prefix + parent + param_prefix + param
        selected = node.css(f_str) if "sel" in kwargs and kwargs["sel"] == "css" else node.xpath(f_str)
        if "is_multiple" in kwargs and kwargs["is_multiple"]:
            return selected.getall()
        return selected.get()
    
    def parse_format_rules(self, node: Selector, **kwargs):
        collected_data = {}
        for key, value in kwargs["format_rules"].items():
            if key not in self.non_formattable_tags:
                try:
                    if "is_cdata" in value and  value["is_cdata"]:
                        collected_data[key] = self.parse_cdata(node, value)
                    else:
                        collected_data[key] = self.extract_values(node=node, **value)
                except Exception as e:
                    if "testing" in kwargs:
                        print(e)
                    self.error_handling(e)
                    continue
        return collected_data, node
    """
    Every link containing node will be extracted here, the formatter
    will be injected from parent function `parse_nodes` which will query
    """

    def parse_node(self, response, node, **kwargs):
        collected_data, node = self.parse_format_rules(node, **kwargs)
        if "testing" in kwargs and kwargs["testing"]:
            yield collected_data, node
        elif "link_store" in kwargs and kwargs["link_store"].is_multiple == True:
            self.create_data(collected_data, kwargs["link_store"], kwargs["source_map"], index= kwargs["index"] if "index" in kwargs else 0)
        else:
            yield self.pack_data_in_container(collected_data, **kwargs)


    """
    Handles different types of error during parsing
    """

    def error_handling(self, e):
        self.log(e)
        pass
