from typing import List
from scrapy import Spider, Request
from scrapy.spiders import XMLFeedSpider
from models.main import  SourceMap, Format,DataLinkContainer


class RSSFeedSpider(XMLFeedSpider):
    name = "rss_feed_spider"
    source_maps: List[SourceMap] = None
    
    """
    Fetch all the sources from digger(db) and sources (collection) where is_rss = True
    Insert all data into self.source_maps, which later will be used to iterate
    """
    def pull_rss_sources_from_db(self):
        None
    
    """
    Find the format using format_id, in digger(db) and formats(collection)
    """
    def find_source_format(format_id: str) -> Format:
        pass
    
    """
    Insert link container in digger(db), sitemaps(collection) in mongodb,
    this collection is transient and will disappear afer use.
    """
    def insert_link_container_in_db(container: DataLinkContainer) -> None:
        pass
    
    """
    Insert article link in digger(db) and travelled(table) in postgres along with indexing.
    This api is used to avoid scraping of duplicate links
    """
    def index_container_link(link: str):
        pass


    """
    Query whether any link exist in digger(db) and travelled(table) in postgres
    """
    def is_article_scrapped(link: str) -> bool:
        pass

    """
    Every Source Map contains links, which are LinkStore containing link and optionaly assumed_tags, and other params.
    If any link store contains their personal formatter, than the formatter will be passed into parser,
    else the default fomatter of source map will be passed
    """
    def crawl_source(source: SourceMap):
        pass

    """
    Synthetic face of parse function, it will need response and [source, formatter] as keyword_args,
    for every link crawled this function will check whether the scraped link exist or not, If exist then pass else
    insert_link_container_in_db will be called, after which index_container_link will also be called
    """
    def parse_response(self, response, **kwargs):
        pass
        

        