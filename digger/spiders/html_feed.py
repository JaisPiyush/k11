from datetime import datetime
from typing import Dict, Generator
from scrapy.spiders import Spider
from models.main import LinkStore, SourceMap, Format, DataLinkContainer
from scrapy_splash import SplashRequest
from utils import is_url_valid


class HTMLFeedSpider(Spider):
    name = "html_feed_spider"
    current_source: SourceMap = None
    current_source_fromatter: Format = None
    format_: Dict = None
    itertag = None
    non_formatables = ['itertag']
    assumed_tags: str = None
    compulsory_tags: str = None

    """
    Pull out all documents which are collection of content links embedded inside html page.
    The find engine will select documents whose `is_rss == False` and `is_collection == True`
    """

    def pull_html_collection_sources_from_db(self) -> Generator[SourceMap, None, None]:
        return SourceMap.pull_all_html_collections()

    """
    This function will pull html source formatter using `format_id == source_map.source_id`
    """

    def _get_html_source_fromat_in_db(self, format_id: str) -> Format:
        pass

    """
    This function will decide which formatting rules to be used for the current link,
    if link_store contains any formatter and the formatter is not equal to default formatter of source
    i.e html_collection_format

    """

    def set_suitable_formatter(self, link_store: LinkStore) -> Dict:
        pass

    """
    Nice wrapper for private api
    """

    def pull_html_source_formatter(self, format_id: str) -> Format:
        return self._get_html_source_fromat_in_db(format_id)

    def set_tag(self, link_store: LinkStore):
        pass

    """
    Each source will be iterated using for loop and each link of the source will be iterated
    after setting the source as `current_source` of the instance and pulled formatter as `current_formatter`
    `formatter` attribute will store rules of format in the current requested link, the format
    will extracted using `set_suitable_formatter` function
    """

    def start_requests(self):
        for source in self.pull_html_collection_sources_from_db():
            self.current_source = source
            self.current_source_fromatter = self.pull_html_source_formatter(
                source.source_id)
            for link_store in self.current_source.links:
                self.format_ = self.get_suitable_formatter(link_store)
                if link_store.link != None and len(link_store.link) > 0 and is_url_valid(link_store.link):
                    self.set_suitable_formatter(link_store)
                    self.set_tag(link_store)
                    if "itertag" in self.format_:
                        self.itertag = self.format_["itertag"]
                    yield SplashRequest(url=link_store.link,
                                        callback=self.parse_without_itertag if self.itertag == None or len(self.itertag) == 0 else self.parse_with_itertag)
                else:
                    pass

    """
    Parser should extract all the fields defined in format, present in the html.
    `get_all()` will fetch all the elements matching with the rules, and yield 
    a packed `DataLinkContainer`
    """

    def parse_without_itertag(self, response, **kwargs):
        collected_data = {}
        length = 0
        for key, value in self.format_.items():
            if key in self.non_formatables:
                pass
            collected_data[key] = response.xpath(
                f'//{value["parent"]}/{value["param"]}').getall()
            if length == 0:
                length = len(collected_data[key])
        for index in range(length):
            return self.pack_data_in_container({key: collected_data[key][index]
                    for key in collected_data.keys()})

    def pack_data_in_container(self, data: Dict) -> DataLinkContainer:
        return DataLinkContainer(container=data,
                                 source_name=self.current_source.source_name, source_id=self.current_source.source_id,
                                 formatter=self.current_source_fromatter.format_id, scraped_on=datetime.now(),
                                 link=data['link'] if 'link' in data else None,
                                 assumend_tags=self.assumed_tags,
                                 compulsory_tags=self.compulsory_tags, watermarks=self.current_source.watermarks,
                                 )

    def parse_nodes(self, response, node):
        pass

    """
    Parser should extract all the itertag elemetns using `get_all()` and pass it to
    `parse_nodes` function to extract and pack data according to format
    """

    def parse_with_itertag(self, response, **kwargs):
        pass
