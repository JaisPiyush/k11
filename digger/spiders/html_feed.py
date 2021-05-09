from datetime import datetime
from typing import Dict, Generator
from vault.exceptions import NoDocumentExists
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

    custom_settings = {
        "ITEM_PIPELINES": {
            "digger.pipelines.CollectionItemDuplicateFilterPiepline": 300,
            "digger.pipelines.CollectionItemSanitizingPipeline": 356,
            "digger.pipelines.CollectionItemVaultPipeline": 412
        }
    }

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
        return Format.adapter().find_one({"$and": [{"format_id": format_id}, {"html_collection_format": {"$exists": True}}]})
       


    """
    This function will decide which formatting rules to be used for the current link,
    if link_store contains any formatter and the formatter is not equal to default formatter of source
    i.e html_collection_format

    """

    def get_suitable_formatter(self, link_store: LinkStore) -> Dict:
        if link_store.formatter != None and len(link_store.formatter) > 0 and link_store.formatter != self.current_source.formatter and link_store.formatter in self.current_source_formatter.extra_formats:
            return self.current_source_fromatter.extra_formats[link_store.formatter]
        return getattr(self.current_source_fromatter, self.current_source.formatter)

    """
    Nice wrapper for private api
    """

    def pull_html_source_formatter(self, format_id: str) -> Format:
        return self._get_html_source_fromat_in_db(format_id)

    def set_tags(self, link_store: LinkStore) -> None:
        if link_store.assumed_tags != None and len(link_store.assumed_tags) > 0:
            self.assumed_tags = link_store.assumed_tags
        else:
            self.assumed_tags = self.current_source.assumed_tags
        if link_store.compulsory_tags != None and len(link_store.compulsory_tags) > 0:
            self.compulsory_tags = link_store.compulsory_tags
        else:
            self.compulsory_tags = link_store.compulsory_tags

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
            if self.current_source_fromatter == None:
                continue
            for link_store in self.current_source.links:
                if link_store.link != None and len(link_store.link) > 0 and is_url_valid(link_store.link):
                    self.format_ = self.get_suitable_formatter(link_store)
                    self.set_tags(link_store)
                    if "itertag" in self.format_:
                        self.itertag = self.format_["itertag"]
                    yield SplashRequest(url=link_store.link,
                                        callback=self.parse_without_itertag if self.itertag == None or len(self.itertag) == 0 else self.parse_with_itertag,
                                        splash_headers={'User-Agent': "Mozilla/5.0 (Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"}
                                        )
                else:
                    pass

    """
    Parser should extract all the fields defined in format, present in the html.
    `get_all()` will fetch all the elements matching with the rules, and yield 
    a packed `DataLinkContainer`
    """

    def parse_without_itertag(self, response, **kwargs) -> Generator[DataLinkContainer, None, None]:
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
                                 assumed_tags=self.assumed_tags,
                                 compulsory_tags=self.compulsory_tags, watermarks=self.current_source.watermarks,
                                 is_formattable=self.current_source.is_structured_aggregator
                                 )

    def parse_nodes(self, response, node) -> DataLinkContainer:
        data = {}
        for key, value in self.format_.items():
            # print(value)
            if key in self.non_formatables:
                continue
            data[key] = node.xpath(
                f'.//{value["parent"]}/{value["param"]}').get()
        return self.pack_data_in_container(data)
            


    """
    Parser should extract all the itertag elemetns using `get_all()` and pass it to
    `parse_nodes` function to extract and pack data according to format
    """

    def parse_with_itertag(self, response, **kwargs) -> Generator[DataLinkContainer, None, None]:
       for node in response.xpath(self.itertag):
           yield self.parse_nodes(response, node)
