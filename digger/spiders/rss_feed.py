from datetime import datetime
from ...common_errors import NoDocumentExists
from typing import Dict, Generator, List
from scrapy import Request
from scrapy.spiders import XMLFeedSpider
from scrapy.utils.spider import iterate_spider_output
from models.main import LinkStore, SourceMap, Format, DataLinkContainer


class RSSFeedSpider(XMLFeedSpider):
    name = "rss_feed_spider"
    source_maps: List[SourceMap] = None
    itertag = 'item'
    formats = None
    non_formattable_tags = ['itertag']
    current_source: SourceMap = None
    current_source_formatters: Format = None
    namespaces = [('dc', 'http://purl.org/dc/elements/1.1/')]

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

    def pull_rss_sources_from_db(self) -> Generator[SourceMap]:
        yield SourceMap.pull_all_rss_models()

    """
    This method will return existing rss format attached with source, otherwise
    the default rss format

    """

    def _get_xml_format_exists_in_db(self, format_id: str) -> Format:
        try:
            return Format.adapter().find_one({"$and": [{"format_id": format_id}, {"xml_collection_format": {"$exist": True}}]})
        except NoDocumentExists:
            return Format.get_default_rss_format()
    """
    Find the format using format_id, in digger(db) and formats(collection) where source.source_id == formatter._id
    """

    def pull_rss_source_formatters(self, format_id: str) -> Format:
        return self._get_xml_format_exists_in_db(format_id)

    """
    This function will get the formatter according to link
    """

    def get_formatter(self, link: str) -> Dict:
        for link_in_source in self.current_source.links:
            if link_in_source.__getattribute__('link') == link and (hasattr(link_in_source, "formatter") and link_in_source.__getattribute__('formatter') is not None):
                return self.current_source_formatters[link_in_source.__getattribute__('formatter')]
        return self.current_source_formatters.__getattribute__(self.current_source.formatter)

    """
    Every Source Map contains links, which are LinkStore containing link and optionaly assumed_tags, and other params.
    If any link store contains their personal formatter, than the formatter will be passed into parser,
    else the default fomatter of source map will be passed
    """

    def start_requests(self):
        sources: List[SourceMap] = self.pull_rss_sources_from_db()
        for source in sources:
            self.current_source = source
            self.current_source_formatters = self.pull_rss_source_formatters(
                source.source_id)
            for link in self.current_source.links:
                yield Request(link.__getattribute__('link'))

    def parse_nodes(self, response, nodes):
        for selector in nodes:
            ret = iterate_spider_output(self.parse_single_node(
                response, selector, self.get_formatter(response.request.url)))
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
                        f"///{self.itertag}{value['parent']}{value['param']}").get()
                except Exception as e:
                    self.error_handling(e)
                    pass
        # if "link" in collected_data and len(collected_data['link']) > 0:
        yield DataLinkContainer(container=collected_data, source_name=self.current_source.source_name, source_id=self.current_source.source_id,
                                formatter=self.current_source_formatters.format_id, scraped_on=datetime.now(), link=collected_data['link'] if 'link' in collected_data else None,
                                watermarks=self.current_source.watermarks)
        # else:
        #     pass

    """
    Handles different types of error during parsing
    """

    def error_handling(self, e):
        self.log(e)
        pass
