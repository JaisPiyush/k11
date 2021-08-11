from datetime import datetime
from mongoengine.queryset.visitor import Q
from k11.models.no_sql_models import Format, DataLinkContainer
from k11.models.sql_models import IndexableArticle
from k11.models.main import ContainerFormat, ContainerIdentity, QuerySelector
from typing import List
from k11.digger.abstracts import BaseSpider
import random
from urllib.parse import urlparse

from scrapy_splash.request import SplashRequest

class ArticleSpider(BaseSpider):

    name="article_spider"
    testing_urls: List[DataLinkContainer] = []

    custom_settings = {
        "ITEM_PIPELINES": {
            "digger.pipelines.ArticlePreprocessor": 298,
            "digger.pipelines.ArticleDuplicateFilter": 300,
            "digger.pipelines.ArticleVaultPipeline": 356
        }
    }

    def get_default_format(self) -> ContainerFormat:
        return ContainerFormat(
            idens=[ContainerIdentity(param="body", is_multiple=False)],
            terminations=[QuerySelector(tag="footer")],
            is_multiple=False
        )

    def get_format(self, container: DataLinkContainer) -> ContainerFormat:
        try: 
            # url = self.get_source_home_url(url)
            formatter: Format = Format.objects(Q(format_id=container.source_id)).no_dereference().first()
            if formatter is None:
                return self.get_default_format()
            return formatter.html_article_format
        except Exception as e:
            self.log(e)
            return self.get_default_format()
    
    def get_scrappable_links(self) -> DataLinkContainer:
        if len(self.testing_urls) > 0:
            return self.testing_urls
        qs = list(DataLinkContainer.objects)
        random.shuffle(qs)
        return qs
    
    def is_article_present_in_db(self, article_link: str) -> bool:
        return IndexableArticle.select().where(IndexableArticle.link == article_link).exists()
    
    def start_requests(self):
        for link_container in self.get_scrappable_links():
            if self.is_article_present_in_db(link_container.link):
                continue
            else:
                self.log(link_container.link +f" is going to be scraped at {datetime.now()}", only_screen=True)
                yield self.consume_link(link_container.link, **{
            "container": link_container,
            "url": link_container.link,
            "is_testing": len(self.testing_urls) > 0
        })
    
    def consume_link(self, link: str, callback=None, **kwargs):
        return SplashRequest(url=link,
        callback=callback if callback is not None else self.parse,
        args={
            "wait": 2.0
        },        splash_headers= {'User-Agent': "Mozilla/5.0 (Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"},
        cb_kwargs=kwargs
        
        )
    
    # def clean_container(self, response, formatter: ContainerFormat):

    
    def parse_content(self, response, formatter: ContainerFormat):
        for identity in formatter.idens:
            content = response.css(identity.param)
            if content != None and len(content) > 0:
                if identity.is_multiple:
                    return content.getall(), identity
                return content.get(), identity
        return response.text, None



    
    def parse(self, response, **kwargs):
        
        try:
            formatter = self.get_format(kwargs["container"])
            content, identity = self.parse_content(response, formatter)
            yield {
                "link_container": kwargs["container"],
                "formatter": formatter,
                "url": kwargs["url"],
                "content": content,
                "iden": identity,
                "is_testing": kwargs["is_testing"]
            }
            # self.log(data, only_screen=True)
            # yield data
        except Exception as err:
            self.log(f"Error from parse  {kwargs} with msg {err} and values {formatter}")
