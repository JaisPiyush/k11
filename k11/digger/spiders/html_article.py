
from k11.models.sql_models import IndexableArticle
from k11.vault import connection_handler
from k11.digger.abstracts import BaseSpider
import json

from typing import Dict, Generator, List, Tuple, Union
from k11.models.main import  ContainerFormat, ContainerIdentity, QuerySelector
from k11.models.no_sql_models import DataLinkContainer, Format
from scrapy.spiders import Spider
from scrapy_splash import SplashRequest
from urllib.parse import ParseResult, urlparse
import os.path
import random


def get_lua_script(name):
    with open(os.path.dirname(__file__) + f'/../lua_modules/{name}', "r") as f:
        return f.read()
SPLASH_ENDPOINT = 'execute'
SPLASH_ARGS = {
    'lua_source': get_lua_script('article_scrap.lua'),
}


class HTMLArticleSpider(BaseSpider):
    name = "html_article_spider"
    current_format: ContainerFormat = None
    current_container: DataLinkContainer = None
    current_url: str = None

    def reset_configs(self):
        self.current_format = None
        self.current_container = None
        self.current_url = None


    custom_settings = {
        "ITEM_PIPELINES": {
            "digger.pipelines.ArticleSanitizer": 298,
            "digger.pipelines.ArticleDuplicateFilter": 300,
            "digger.pipelines.ArticleVaultPipeline": 356
        }
    }

    def get_test_format(self) -> ContainerFormat:
        return ContainerFormat(
            idens=[ContainerIdentity(param=".thumb-image", is_multiple=True)],
            is_multiple=False
        )
    
    def get_default_format(self) -> ContainerFormat:
        return ContainerFormat(
            idens=[ContainerIdentity(param="body", is_multiple=False)],
            terminations=[QuerySelector(tag="footer")],
            is_multiple=False
        )
    
    """
    Get article formatter associated with source_home_link,
    choose default html_article_format if url.part[:-1] is not present in extra_formats
    """
    def get_format(self, url: str) -> Tuple[str, Union[Format, None]]:
        parsed: ParseResult = urlparse(url)
        # print(f"{parsed.scheme}://{parsed.netloc}")
        try:
            format_: Union[Format, None] = Format.objects.get(source_home_link = f"{parsed.scheme}://{parsed.netloc}")
            
            if len(parsed.path) > 0 and format_.extra_formats != None:
                ls = parsed.path.split("/")
                ls = [txt for txt in ls if len(txt) > 0]
                formatter = None
                for index in range(len(ls)):
                    if (key := "/".join(ls[:len(ls) - index])) in format_.extra_formats:
                        # return ContainerFormat.from_dict_to_json(**format_.extra_formats[key])
                        formatter = ContainerFormat.from_dict(**format_.extra_formats[key])
            else:
                formatter = format_.html_article_format
            return formatter.to_json_str() if formatter is not None else json.dump({}), formatter
        except Exception as e:
            return json.dumps({}), None

        
   
    """
    Pull out all scrappable data link containers out of the database
    """
    def get_scrappable_links(self):
        qs = list(DataLinkContainer.objects)
        random.shuffle(qs)
        return qs

 
    
    # Return True if article is present inside the database
    def is_article_present_in_db(self, article_link: str) -> bool:
        return IndexableArticle.select().where(IndexableArticle.article_id == article_link).exists()

    def start_requests(self):
        for container in self.get_scrappable_links():
            # self.reset_configs()
            
            if self.is_article_present_in_db(container.link):
                continue
            else:
                # self.current_container = container
                self.log(container.link +" is in the process", only_screen=True)
                format_str, format_ = self.get_format(container.link)
                SPLASH_ARGS['format'] = format_str         
                
                yield SplashRequest(url=container.link,
        callback=self.parse, endpoint=SPLASH_ENDPOINT,args=SPLASH_ARGS,
        splash_headers= {'User-Agent': "Mozilla/5.0 (Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"},
        cb_kwargs={
            "container": container,
            "format": format_,
            "url": container.link
        }
        
        )

    def parse(self, response, **kwargs):
        body = json.loads(response.body)
        if body is None:
            pass
        else:
            yield {
            "container": kwargs["container"],
            "format": kwargs["format"],
            "content": body['html'],
            "disabled": body['disabled'],
            "iden": body['iden'] if 'iden' in body else 'body',
            "url": kwargs["url"]
        }

        