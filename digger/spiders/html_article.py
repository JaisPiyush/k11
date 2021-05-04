from hashlib import sha256
from random import randint
from typing import Dict, Generator, List, Tuple, Union
from models.main import ArticleContainer, ContainerFormat, ContainerIdentity, DataLinkContainer, Format, QuerySelector, SourceMap
from scrapy.spiders import Spider
from scrapy_splash import SplashRequest
from utils import get_lua_script
from urllib.parse import urlparse


SPLASH_ENDPOINT = 'execute'
SPLASH_ARGS = {
    'lua_source': get_lua_script('scraps.lua'),
}


class HTMLArticleSpider(Spider):
    name = "html_article_spider"
    current_format = None
    current_container = None

    def get_test_format(self) -> ContainerFormat:
        return ContainerFormat(
            idens=[ContainerIdentity(param=".thumb-image", is_multiple=True)],
            is_multiple=False
        )
    
    """
    Get article formatter associated with source_home_link,
    choose default html_article_format if url.part[:-1] is not present in extra_formats
    """
    def get_format(self, url: str) -> ContainerFormat:
        pass
   
    """
    Pull out all scrappable data link containers out of the database
    """
    def get_scrappable_links(self) -> Generator[DataLinkContainer, None, None]:
        pass

    def start_requests(self):
        for container in self.get_scrappable_links():
            self.current_container = container
            self.current_format = self.get_format(container.link)
            SPLASH_ARGS['format'] = self.current_format
            yield SplashRequest(url=container.link,
        callback=self.parse, endpoint=SPLASH_ENDPOINT,args=SPLASH_ARGS,
        splash_headers= {'User-Agent': "Mozilla/5.0 (Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"}
        
        )
    
    @staticmethod
    def is_source_present_in_db(home_link: str) -> bool:
        return Format.adapter().find_one({"source_home_link": home_link}) != None
    
    """
    Parse multiple articles in case of is_multiple=True, the scrapped data contains List of articles,
    the parser has to pack all the given articles in different ArticleContainer.
    """
    def parse_multiple(self, ls: List[str]):
        pass

    
    """
    Function will nicely wrap content into ArticleContainer along with tags and meta infos
    """
    def pack_container(self, url, content=None, index=0, tags=[],) -> ArticleContainer:
        parsed_link = urlparse(url)
        container = ArticleContainer(
            article_id=sha256(url).hexdigest() + str(index) if index > 0 else "",
            source_name=self.current_container.source_name,
            source_id=self.current_container.source_id,
            article_link=url,
            creator=self.current_container.container['creator'] if 'creator' in self.current_container.container else None,
            home_link=f"{parsed_link.scheme}://{parsed_link.netloc}",
            site_name=self.current_container.container['site_name'] if 'site_name' in self.current_container.container else self.current_container.source_name,
            pub_date=self.current_container.container['pub_date'] if 'pub_date' in self.current_container.container else None,
            scraped_on=self.current_container.scraped_on,
            content=content,
            is_source_present_in_db=self.is_source_present_in_db(f"{parsed_link.scheme}://{parsed_link.netloc}"),
            tags=tags,
        )
        return container

    def parse(self, response, **kwargs):
        pass

        