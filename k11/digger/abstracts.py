from typing import Dict, Generator, List, Optional, Tuple, Union
from scrapy import Spider, Selector
from k11.models.main import DataLinkContainer, Format, SourceMap, LinkStore
from scrapy import Request


ParsedNodeValue = Optional[Union[Tuple[Dict, Selector], Generator[DataLinkContainer, None, None]]]


class AbstractCollectionScraper(Spider):

    
    """
    Format class can contain tons of formatting rules for any given source,
    Link Stores have ability to store special format rules for themselves.
    This function will check existance of format rules in the formatter
    if found None, provided default value will be used to extract the formatter
    IF default is also None then we will set source map's default formatter as it's value
    """
    def get_suitable_format_rules(self, formats: Format, source: SourceMap, link_store: LinkStore, default = None) -> Dict: ...

    """
    Source Map Object contains all the links and prequisting data,
    we will fetch all realted source maps as generator
    """
    def get_sources_from_database(self) -> Generator[SourceMap, None, None]: ...
    
    """
    Every source map is connected with the formatter which is a bank of formating rules,
    Formating rule is record of data name and query text for extraction
    """
    def get_formatter_from_database(self, format_id: str) -> Format: ...
    
    """
    Source map contains assumed_tags and compulsory tags by default, although they can be None.
    It's possible that many links don't comply with these genral tags so, link stores can store 
    assumed_tags and compulsory tags of their own.
    This function decides which tags has to be selected for further procedure.
    """
    def get_tags_for_link_store(self, source: SourceMap, link_store: LinkStore) -> Tuple[str, List[str]]: ...
    
    """
    `start_requests` is the most important function in scrapy spiders,
    they send requests and crawl all received data.
    This function shorts all the general code for start requests.
    For any customization, `before_requesting` allows users to customize prepared data just before request
    """
    def run_requests(self) -> Union[Request, Generator[Request, None, None]]: ... 
    
    # Implements link_store logic in run_requests
    def process_link_store(self, link_store: LinkStore, source_map: SourceMap, formats: Format) -> Request: ...

    def before_requesting(self, **kwargs) -> Dict:
        return kwargs
    
    # Nice wrapper around Splash Request
    def call_request(self, url: str, callback, source: SourceMap, format_rules: Dict, formats: Format, assumed_tags: str, compulsory_tags: List[str], splash_headers: Dict[str, str], **kwargs): ...
