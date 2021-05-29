from scrapy import Selector
from .base import BaseCollectionScraper
from traceback import format_exc
from typing import Dict, Generator, List
from k11.models.main import SourceMap, Format, DataLinkContainer
from k11.utils import is_url_valid


#TODO: Need to implement collection to article in this

class HTMLFeedSpider(BaseCollectionScraper):
    name = "html_feed_spider"
    itertag = None
    non_formatables = ['itertag']

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
    Nice wrapper for private api
    """

    def pull_html_source_formatter(self, format_id: str) -> Format:
        return self._get_html_source_fromat_in_db(format_id)

    """
    Each source will be iterated using for loop and each link of the source will be iterated
    after setting the source as `current_source` of the instance and pulled formatter as `current_formatter`
    `formatter` attribute will store rules of format in the current requested link, the format
    will extracted using `set_suitable_formatter` function
    """

    def start_requests(self):
        for source in self.pull_html_collection_sources_from_db():
            formats = self.pull_html_source_formatter(
                source.source_id)
            if formats == None:
                continue
            for link_store in source.links:
                if link_store.link != None and len(link_store.link) > 0 and is_url_valid(link_store.link):
                    format_rules = self.get_suitable_format_rules(formats, source, link_store, default="html_collection_format")
                    assumed_tags, compulsory_tags = self.get_tags(source,link_store)
                    yield self.call_request(url=link_store.link,
                    callback=self.parse,
                    source=source, format_rules=format_rules,formats=formats,
                    assumed_tags=assumed_tags, compulsory_tags=compulsory_tags
                    )
                else:
                    continue


    def parse(self, response, **kwargs):
        if "itertag" in kwargs["format_rules"] and kwargs["format_rules"]["itertag"] != None:
            return self.parse_with_itertag(response, **kwargs)
        return self.parse_without_itertag(response, **kwargs)

    """
    Parser should extract all the fields defined in format, present in the html.
    `get_all()` will fetch all the elements matching with the rules, and yield 
    a packed `DataLinkContainer`
    """

    def parse_without_itertag(self, response, **kwargs) -> Generator[DataLinkContainer, None, None]:
        collected_data = {}
        length = 0
        for key, value in kwargs['format_rules'].items():
            if key in self.non_formatables:
                continue
            collected_data[key] = response.xpath(
                f'//{value["parent"]}/{value["param"]}').getall()
            if length == 0:
                length = len(collected_data[key])
        for index in range(length):
            yield self.pack_data_in_container({key: collected_data[key][index]
                    for key in collected_data.keys() if collected_data[key][index] != None})
    
    def register_namespaces(self, node: Selector):
        for namespace in self.namespaces:
            node.register_namespace(namespace[0], namespace[1])

    def parse_nodes(self, response, node, **kwargs) -> DataLinkContainer:
        data = {}
        for key, value in kwargs['format_rules'].items():
            # print(value)
            if key in self.non_formatables:
                continue
            data[key] = node.xpath(
                f'.//{value["parent"]}/{value["param"]}').get()
        return self.pack_data_in_container(data, **kwargs)
            


    """
    Parser should extract all the itertag elemetns using `get_all()` and pass it to
    `parse_nodes` function to extract and pack data according to format
    """

    def parse_with_itertag(self, response, **kwargs) -> Generator[DataLinkContainer, None, None]:
        if kwargs["url"] == "https://expertphotography.com/category/creative-projects-challenges/":
            self.log(self.itertag)
        for node in response.xpath('//' + kwargs["format_rules"]["itertag"]):
            self.log(node)
            yield self.parse_nodes(response, node, **kwargs)
