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


SPLASH_ENDPOINT = 'execute'
SPLASH_ARGS = {
    'lua_source': get_lua_script('scraps.lua'),
}


class HTMLArticleSpider(Spider):
    name = "html_article_spider"
    current_container: DataLinkContainer = None

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
                        return ContainerFormat.from_dict(**format_.extra_formats[key]).to_json_str()
            return format_.html_article_format.to_json_str()
        except NoDocumentExists as e:
            return json.dumps({})

        
   
    """
    Pull out all scrappable data link containers out of the database
    """
    def get_scrappable_links(self) -> Generator[DataLinkContainer, None, None]:
        return DataLinkContainer.get_all()

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
    def parse_multiple(self,container: DataLinkContainer, url: str, ls: List[str], redirection_required: bool=False) -> Generator[ArticleContainer, None, None]:
        for index, article_ls in enumerate(ls):
            data = self.extract_content(article_ls)
            data['index'] = index
            data['tags'] = container.assumend_tags.split(" ")
            yield self.pack_container(container, url,**data)
            
    def parse_unknown_source(self, container: DataLinkContainer, url: str, body: str) -> ArticleContainer:
        data =self.extract_content(body)
        data['index'] = 0
        data['tags'] = container.assumend_tags.split(" ")
        return self.pack_container(container, url, **data )

    """
    Function will nicely wrap content into ArticleContainer along with tags and meta infos
    """
    def pack_container(self,container: DataLinkContainer, url: str,images: List[str]=[], 
      videos: List[str]=[], text: str=None, content: str=None, index: int=0, tags: List[str]=[],) -> ArticleContainer:
        parsed_link = urlparse(url)
        container = ArticleContainer(
            article_id=sha256(url).hexdigest() + str(index) if index > 0 else "",
            source_name=container.source_name,
            source_id=container.source_id,
            article_link=url,
            creator=container.container['creator'] if 'creator' in container.container else None,
            home_link=f"{parsed_link.scheme}://{parsed_link.netloc}",
            site_name=container.container['site_name'] if 'site_name' in container.container else container.source_name,
            pub_date=container.container['pub_date'] if 'pub_date' in container.container else None,
            scraped_on=container.scraped_on,
            content=content,
            is_source_present_in_db=self.is_source_present_in_db(f"{parsed_link.scheme}://{parsed_link.netloc}"),
            tags=tags,
            compulsory_tags=container.compulsory_tags if container.compulsory_tags is not None else [],
            images=images,
            videos=videos,
            text=text,
            title=container.container['title']
        )
        return container
    

    def extract_content(self, body) -> Dict[str, Union[str, List[str], None]]:
        data = {"images": [], "videos": [], "text": None, "content": body}
        selector = Selector(text=body)
        data['images'] = selector.css('img::attr(src)').getall()
        # Picture tags
        data['images'] += selector.xpath('///picture//source/@src').getall()
        data['videos'] = selector.css('video::attr(src)').getall()
        data['text'] = "".join([txt for text in selector.xpath('///text()').getall() if len((txt := text.replace("\n", "").replace("\t", ""))) > 0])
        return data


    def parse(self, response, **kwargs):
        url = response.request.url
        body = json.loads(response.body)
        if isinstance(body, str):
            # Unknown Source
            return self.parse_unknown_source(self.current_container,url, body)
        # known source
        return self.parse_multiple(self.current_container,url, body)

        