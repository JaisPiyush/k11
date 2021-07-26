from k11.digger.abstracts import ParsedNodeValue
from mongoengine.queryset.visitor import Q
from .base import BaseCollectionScraper, BaseContentExtraction
from typing import Generator
from k11.models.no_sql_models import Format, SourceMap
from k11.vault.app import connection_handler

class HTMLFeedSpider(BaseCollectionScraper, BaseContentExtraction):
    name = "html_feed_spider"
    itertag = None
    default_format_rules = "html_collection_format"


    custom_settings = {
        "ITEM_PIPELINES": {
            "digger.pipelines.CollectionItemDuplicateFilterPipeline": 300,
            "digger.pipelines.CollectionItemSanitizingPipeline": 356,
            "digger.pipelines.CollectionItemVaultPipeline": 412
        }
    }

    """
    Pull out all documents which are collection of content links embedded inside html page.
    The find engine will select documents whose `is_rss == False` and `is_collection == True`
    """

    def get_sources_from_database(self) -> Generator[SourceMap, None, None]:
        return SourceMap.objects.pull_all_html_models()
 
    """
    This function will pull html source formatter using `format_id == source_map.source_id`
    """

    def _get_html_source_fromat_in_db(self, format_id: str) -> Format:
        try:
            return Format.objects(Q(format_id=format_id) & Q(html_collection_format__exists=True)).get()
        except Exception as e:
            self.log(f"{format_id} is showing error in html_source_extraction {e}")
            raise e
        
       

    """
    Nice wrapper for private api
    """

    def get_formatter_from_database(self, format_id: str) -> Format:
        return self._get_html_source_fromat_in_db(format_id)

    def _parse(self, response, **kwargs):
        if "itertag" in kwargs["format_rules"] and kwargs["format_rules"]["itertag"] != None:
            return self.parse_with_itertag(response, **kwargs)
        return self.parse_without_itertag(response, **kwargs)

    """
    Parser should extract all the fields defined in format, present in the html.
    `get_all()` will fetch all the elements matching with the rules, and yield 
    a packed `DataLinkContainer`
    """

    def parse_without_itertag(self, response, **kwargs) -> ParsedNodeValue:
        collected_data = {}
        length = 0
        nodes = []
        for key, value in kwargs['format_rules'].items():
            data, node = self.extract_values(response, value["parent"], param=value["param"], param_prefix="//", is_multiple=True) 
            collected_data[key] = data
            nodes.append(node)
            if length == 0:
                length = len(data)
        for index in range(length):
            return self.process_extracted_data({key: collected_data[key][index]
                    for key in collected_data.keys() if collected_data[key][index] != None},nodes[index],index=index, **kwargs)
    
    """
    Parser should extract all the itertag elemetns using `get_all()` and pass it to
    `parse_nodes` function to extract and pack data according to format
    """

    def parse_with_itertag(self, response, **kwargs) -> ParsedNodeValue:
        for index, node in enumerate(response.xpath('//' + kwargs["format_rules"]["itertag"])):
            return self._parse_node(response, node,index=index, **kwargs)
