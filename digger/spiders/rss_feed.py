from datetime import datetime
from utils import is_url_valid
from vault.exceptions import NoDocumentExists
from typing import Dict, Generator, List
from scrapy import Request
from scrapy.spiders import XMLFeedSpider
from scrapy.utils.spider import iterate_spider_output
from models.main import  LinkStore, SourceMap, Format, DataLinkContainer


class RSSFeedSpider(XMLFeedSpider):
    name = "rss_feed_spider"
    itertag = 'item'
    non_formattable_tags = ['itertag']
    current_source: SourceMap = None
    current_source_formatter: Format = None
    namespaces = [('dc', 'http://purl.org/dc/elements/1.1/')]
    format_ : Dict = None
    assumed_tags: str = None
    compulsory_tags: str = None

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
    This function will get the formatter according to link
    """

    def set_formatter(self, link_store: LinkStore) -> None:
        if link_store.formatter != None and len(link_store.formatter) > 0 and link_store.formatter != self.current_source.formatter and link_store.formatter in self.current_source_formatter.extra_formats:
            self.format_ = self.current_source_formatter.extra_formats[link_store.formatter]
        self.format_ =  getattr(self.current_source_formatter, self.current_source.formatter)
    
    def set_tags(self, link_store: LinkStore) -> None:
        if link_store.assumed_tags != None and len(link_store.assumed_tags) > 0:
            self.assumed_tags = link_store.assumed_tags
        else:
            self.assumed_tags = self.current_source.assumed_tags
        if link_store.compulsory_tags != None and len(link_store.compulsory_tags) > 0:
            self.compulsory_tags = link_store.compulsory_tags
        else:
            self.compulsory_tags = self.current_source.compulsory_tags

    """
    Every Source Map contains links, which are LinkStore containing link and optionaly assumed_tags, and other params.
    If any link store contains their personal formatter, than the formatter will be passed into parser,
    else the default fomatter of source map will be passed
    """

    def start_requests(self):
        for source in self.pull_rss_sources_from_db():
            self.current_source = source
            # print(source)
            self.current_source_formatter = self.pull_rss_source_formatters(
                source.source_id)
            if self.current_source_formatter == None:
                continue
            for link_store in self.current_source.links:
                if link_store.link != None and len(link_store.link) > 0 and is_url_valid(link_store.link):
                    self.set_formatter(link_store)
                    self.set_tags(link_store)
                    # print(link_store.link, self.format_)
                    if "itertag" in self.format_:
                        self.itertag = self.format_["itertag"]
                    yield Request(getattr(link_store, "link"))
                else:
                    pass

    def parse_nodes(self, response, nodes):
        for selector in nodes:
            ret = iterate_spider_output(self.parse_single_node(
                response, selector, self.format_))
            for result_item in self.process_results(response, ret):
                yield result_item

    """
    Every link containing node will be extracted here, the formatter
    will be injected from parent function `parse_nodes` which will query
    """

    def parse_single_node(self, response, node, format_: Dict):
        collected_data = {}
        for key, value in format_.items():
            if key not in self.non_formattable_tags:
                try:
                    collected_data[key] = node.xpath(
                        f"///{self.itertag}//{value['parent']}/{value['param']}").get()
                except Exception as e:
                    self.error_handling(e)
                    pass
        # if "link" in collected_data and len(collected_data['link']) > 0:s
        yield DataLinkContainer(container=collected_data, source_name=self.current_source.source_name, source_id=self.current_source.source_id,
                                formatter=self.current_source_formatter.format_id, scraped_on=datetime.now(), 
                                link=collected_data['link'] if 'link' in collected_data else None,
                                assumend_tags=self.assumed_tags, compulsory_tags=self.compulsory_tags,
                                watermarks=self.current_source.watermarks)
        # else:
        #     pass

    """
    Handles different types of error during parsing
    """

    def error_handling(self, e):
        self.log(e)
        pass
