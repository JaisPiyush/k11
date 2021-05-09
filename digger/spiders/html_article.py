from hashlib import sha256
import json

from vault.exceptions import NoDocumentExists
from scrapy.selector import Selector
from typing import Dict, Generator, List, Union
from models.main import ArticleContainer, ContainerFormat, ContainerIdentity, DataLinkContainer, Format, QuerySelector
from scrapy.spiders import Spider
from scrapy_splash import SplashRequest
from utils import get_lua_script
from urllib.parse import ParseResult, urlparse
from w3lib.html import remove_tags_with_content, remove_comments, replace_escape_chars

SPLASH_ENDPOINT = 'execute'
SPLASH_ARGS = {
    'lua_source': get_lua_script('article_scrap.lua'),
}


class HTMLArticleSpider(Spider):
    name = "html_article_spider"
    current_format: ContainerFormat = None
    current_container: DataLinkContainer = None


    custom_settings = {
        "ITEM_PIPELINES": {
            "digger.pipelines.ArticleDuplicateAndContentTypeFilter": 300,
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
        return DataLinkContainer.adapter().find({"link":"https://listverse.com/2021/04/25/top-10-animals-you-thought-were-extinct-but-arent/"})

    def start_requests(self):
        for container in list(self.get_scrappable_links()):
            # print(container.container)
            self.current_container = container
            format_ = self.get_format(container.link)
            SPLASH_ARGS['format'] = format_
            yield SplashRequest(url=container.link,
        callback=self.parse, endpoint=SPLASH_ENDPOINT,args=SPLASH_ARGS,
        splash_headers= {'User-Agent': "Mozilla/5.0 (Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"}
        
        )
    
    @staticmethod
    def is_source_present_in_db(home_link: str) -> bool:
        return Format.adapter().find_one({"source_home_link": home_link}, silent=True) != None
    
    """
    Parse multiple articles in case of is_multiple=True, the scrapped data contains List of articles,
    the parser has to pack all the given articles in different ArticleContainer.
    """
    def parse_multiple(self,container: DataLinkContainer, url: str, body: Dict, redirection_required: bool=False) -> Generator[ArticleContainer, None, None]:
        for index, article_ls in enumerate(body['html']):
            data = self.extract_content(article_ls, disabled=body['disabled'],container=container)
            data['index'] = index
            data['tags'] = container.assumed_tags.split(" ")
            yield self.pack_container(container, url,**data)
            
    def parse_single_content(self, container: DataLinkContainer, url: str, body: Dict) -> ArticleContainer:
        data =self.extract_content(body['html'], disabled=body['disabled'],container=container)
        data['index'] = 0
        data['tags'] = container.assumed_tags.split(" ")
        return self.pack_container(container, url, **data )
    
    def get_title(self, selector: Selector, container: DataLinkContainer = None, ) -> str:
        if container is not None and "title" in container.container:
            return container.container["title"]
        elif self.current_format != None and self.current_format.title_selector != None:
            return selector.xpath(self.current_format.title_selector).get()
        return ""
    
    def get_creator(self, selector: Selector, container: DataLinkContainer = None, ) -> str:
        if container is not None and "creator" in container.container:
            return container.container["creator"]
        elif self.current_format != None and self.current_format.creator_selector != None:
            return selector.xpath(self.current_format.creator_selector).get()
        return container.source_name

    """
    Function will nicely wrap content into ArticleContainer along with tags and meta infos
    """
    def pack_container(self, container: DataLinkContainer, url: str,images: List[str]=[],title:str="",
      creator:str= '', disabled: List[str] = [], videos: List[str]=[], text_set: List[str]=None, content: str=None, 
      index: int=0, tags: List[str]=[],) -> ArticleContainer:
        parsed_link = urlparse(url)
        container = ArticleContainer(
            article_id=sha256(url).hexdigest() + str(index) if index > 0 else "",
            source_name=container.source_name,
            source_id=container.source_id,
            article_link=url,
            creator=creator,
            home_link=f"{parsed_link.scheme}://{parsed_link.netloc}",
            site_name=container.container['site_name'] if 'site_name' in container.container else container.source_name,
            pub_date=container.container['pub_date'] if 'pub_date' in container.container else None,
            scraped_on=container.scraped_on,
            content=content,
            disabled=disabled,
            is_source_present_in_db=self.is_source_present_in_db(f"{parsed_link.scheme}://{parsed_link.netloc}"),
            tags=tags,
            compulsory_tags=container.compulsory_tags if container.compulsory_tags is not None else [],
            images=images,
            videos=videos,
            text_set=text_set,
            title=title
        )
        return container
    

    def simple_cleansing(self, body:str) -> str:
        body = remove_tags_with_content(remove_comments(body), which_ones=('b', 'script', 'style', 'noscript'))
        body = replace_escape_chars(body)
        return body
    

    def extract_content(self, body:str, disabled: List[str] = [], container: DataLinkContainer = None) -> Dict[str, Union[str, List[str], None]]:
        data = {"images": [], "videos": [], "text_set": None, "content": body, "disabled": disabled}
        selector = Selector(text=self.simple_cleansing(body))
        data['images'] = selector.css('img::attr(src)').getall()
        # Picture tags
        data['images'] += selector.xpath('///picture//source/@src').getall()
        data['videos'] = selector.css('video::attr(src)').getall()
        data['text_set'] = []
        # for text in selector.xpath('///text()').getall():
        if len(txt := selector.xpath('///text()').getall()) > 0:
            if len(txt) < 245:
                data["text_set"].append(txt)
            else:
                for chunk in range(0, len(txt), 245):
                    if chunk + 245 < len(txt):
                        data["text_set"].append(txt[chunk: chunk + 245])
                    else:
                        data["text_set"].append(txt[chunk:])
        data["title"] = self.get_title(selector, container=container)
        data["creator"] = self.get_creator(selector, container=container)
        data['content'] = body
        return data


    def parse(self, response, **kwargs):
        url = response.request.url
        body = json.loads(response.body)
        yield {
            "container": self.current_container,
            "format": self.current_format,
            "content": body['html'],
            "disabled": body['disabled'],
            "iden": body['iden'],
            "url": url
        }

        