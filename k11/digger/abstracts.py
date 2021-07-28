from datetime import datetime
import logging
# from digger.logger import setup_loggers
from typing import Dict, Generator, List, Optional, Tuple, Union
from urllib.parse import urlparse
from scrapy import signals
from scrapy import Spider, Selector
from k11.models.main import LinkStore
from k11.models.no_sql_models import DataLinkContainer, Format, SourceMap, ErrorLogs
from scrapy import Request
from k11.vault import connection_handler


ParsedNodeValue = Optional[Union[Tuple[Dict, Selector], Generator[DataLinkContainer, None, None]]]


logging.basicConfig(filename="scrap_error.log", format="%(asctime)s %(levelname)s: %(message)s", level=logging.ERROR)




class BaseSpider(Spider):

    sql_session = None
    ObjectManager = None

    def __init__(self, *arg, **kwargs):
        # logger = setup_loggers(self.name)
        super().__init__(*arg, **kwargs)
    
    
    def spider_closed(self):
        connection_handler.disconnect_mongo_engines()
        connection_handler.dispose_sql_engines()
        self.spider_close()
    
    def spider_opened(self):
        # Almost all spiders use MongoDB. So, its better to create them all here and spider_open hook can be 
        # used to manipulate Postgres
        connection_handler.mount_mongo_engines()
        self.spider_open()
    
    def spider_open(self): ...
    def spider_close(self): ...
    

    @staticmethod
    def get_source_home_url(url):
        parsed_url = urlparse(url)
        return parsed_url.scheme + '://' + parsed_url.netloc
    
    def log(self,message, level=logging.ERROR, only_screen=False, **kwargs):
        self.logger.log(level, message, **kwargs)
        if not only_screen and level >= logging.ERROR:

            try:
                error = ErrorLogs(time=datetime.now(), level=level, message=message)
                error.save()
            except Exception as e:
                pass
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider =  super(BaseSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    

class AbstractCollectionScraper(BaseSpider):

    
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
