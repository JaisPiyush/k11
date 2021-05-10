
import json

from vault.exceptions import NoDocumentExists
from typing import Dict, Generator, List, Union
from models.main import  ContainerFormat, ContainerIdentity, DataLinkContainer, Format, QuerySelector
from scrapy.spiders import Spider
from scrapy_splash import SplashRequest
from utils import get_lua_script
from urllib.parse import ParseResult, urlparse

SPLASH_ENDPOINT = 'execute'
SPLASH_ARGS = {
    'lua_source': get_lua_script('article_scrap.lua'),
}


class HTMLArticleSpider(Spider):
    name = "html_article_spider"
    current_format: ContainerFormat = None
    current_container: DataLinkContainer = None
    current_url: str = None


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
    def get_format(self, url: str) -> str:
        parsed: ParseResult = urlparse(url)
        # print(f"{parsed.scheme}://{parsed.netloc}")
        try:
            format_: Union[Format, None] = Format.adapter().find_one({"source_home_link": f"{parsed.scheme}://{parsed.netloc}"})
            if len(parsed.path) > 0 and format_.extra_formats != None:
                ls = parsed.path.split("/")
                ls = [txt for txt in ls if len(txt) > 0]
                for index in range(len(ls)):
                    if (key := "/".join(ls[:len(ls) - index])) in format_.extra_formats:
                        # return ContainerFormat.from_dict_to_json(**format_.extra_formats[key])
                        self.current_format = ContainerFormat.from_dict(**format_.extra_formats[key])
            else:
                self.current_format = format_.html_article_format
            return self.current_format.to_json_str()
        except NoDocumentExists as e:
            return json.dumps({})

        
   
    """
    Pull out all scrappable data link containers out of the database
    """
    def get_scrappable_links(self) -> Generator[DataLinkContainer, None, None]:
        return DataLinkContainer.adapter().find({})

    def start_requests(self):
        for container in list(self.get_scrappable_links()):
            # print(container.container)
            self.current_container = container
            format_ = self.get_format(container.link)
            SPLASH_ARGS['format'] = format_
            self.current_url = container.link
            yield SplashRequest(url=container.link,
        callback=self.parse, endpoint=SPLASH_ENDPOINT,args=SPLASH_ARGS,
        splash_headers= {'User-Agent': "Mozilla/5.0 (Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"}
        
        )

    def parse(self, response, **kwargs):
        body = json.loads(response.body)
        yield {
            "container": self.current_container,
            "format": self.current_format,
            "content": body['html'],
            "disabled": body['disabled'],
            "iden": body['iden'],
            "url": self.current_url
        }

        