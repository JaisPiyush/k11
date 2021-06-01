from datetime import datetime

from scrapy.http.request import Request
from k11.utils import is_url_valid
from k11.digger.abstracts import AbstractCollectionScraper, ParsedNodeValue
from typing import Dict, Generator, List, Tuple, Union
from scrapy.spiders import Spider
from scrapy import Selector
from urllib.parse import urlparse
from scrapy_splash.request import SplashRequest
from k11.models.main import Format, LinkStore, SourceMap, DataLinkContainer, ContentType, ArticleContainer
from urllib.parse import urlparse
from hashlib import sha256




class BaseCollectionScraper(AbstractCollectionScraper):

    default_format_rules = None

    def get_suitable_format_rules(self, formats: Format, source: SourceMap, link_store: LinkStore, default="") -> Dict:
        format_rules = None
        if link_store.formatter != None and len(link_store.formatter) > 0 and link_store.formatter != source.formatter and link_store.formatter in formats.extra_formats:
            format_rules = formats.extra_formats[link_store.formatter]
        else:
            format_rules =  getattr(formats, default)
        return format_rules
    
    def get_tags_for_link_store(self, source: SourceMap, link_store: LinkStore) -> Tuple[str, List[str]]:
        assumed_tags, compulsory_tags = "", []
        if link_store.assumed_tags != None and len(link_store.assumed_tags) > 0:
            assumed_tags = link_store.assumed_tags
        else:
            assumed_tags = source.assumed_tags
        if link_store.compulsory_tags != None and len(link_store.compulsory_tags) > 0:
            compulsory_tags = link_store.compulsory_tags
        else:
            compulsory_tags = link_store.compulsory_tags
        return assumed_tags, compulsory_tags

    def call_request(self, url: str, callback, source: SourceMap, format_rules: Dict, formats: Format, assumed_tags: str, compulsory_tags: List[str],
                            splash_headers={'User-Agent': "Mozilla/5.0 (Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"}, link_store: LinkStore = None, **kwargs):
                            if "cb_kwargs" not in kwargs:
                                kwargs['cb_kwargs'] = {}
                            kwargs['cb_kwargs'].update({
                                "source": source,
                                "format_rules": format_rules,
                                "formats": formats,
                                "assumed_tags": assumed_tags,
                                "compulsory_tags": compulsory_tags,
                                "url": url,
                                "link_store": link_store
                            })
                            return SplashRequest(url=url, args={"wait": 0.8}, callback=callback, splash_headers=splash_headers, **kwargs)

    
    def process_link_store(self, link_store: LinkStore, source: SourceMap, formats: Format, **kwargs) -> Request:
        format_rules = self.get_suitable_format_rules(formats=formats, source=source, link_store=link_store,default=self.default_format_rules)
        assumed_tags, compulsory_tags = self.get_tags_for_link_store(source=source, link_store=link_store)
        data = self.before_requesting(url=link_store.link, callback=self._parse, formats=formats, format_rules=format_rules, source=source, 
        assumed_tags=assumed_tags, compulsory_tags=compulsory_tags, link_store=link_store)
        if "cb_kwargs" not in data:
            data["cb_kwargs"] = {}
        data["cb_kwargs"].update(kwargs)
        return self.call_request(**data)

    def run_requests(self, **kwargs):
        for source in self.get_sources_from_database():
            formats = self.get_formatter_from_database(source.source_id)
            if formats == None:
                continue
            for link_store in source.links:
                if link_store.link != None and len(link_store.link) > 0 and is_url_valid(link_store.link):
                    yield self.process_link_store(link_store, source, formats, **kwargs)
                else:
                    continue
    
    def start_requests(self, **kwargs):
        return self.run_requests(**kwargs)
        

class BaseContentExtraction:

    non_formattable_tags = ['itertag', 'namespaces']

    """
    Handles different types of error during parsing
    """

    def error_handling(self, e): ...

    def create_article(self, data: Dict, link_store: LinkStore, source_map: SourceMap, index: int = 0):
        if ArticleContainer.adapter().find_one({"article_link" : data["link"]}, silent=True) == None:
            ArticleContainer.adapter().create(**self.process_single_article_data(data=data, link_store=link_store, source_map=source_map, index=index))

    def process_single_article_data(self, data: Dict, link_store: LinkStore, source_map: SourceMap, index : int = 0):
        data["content_type"] = link_store.content_type
        if link_store.compulsory_tags is not None:
            data["compulsory_tags"] = link_store.compulsory_tags
        if link_store.assumed_tags is not None:
            data["assumed_tags"] = link_store.assumed_tags.split(" ")
        data["index"] = index
        data["scrap_link"] = link_store.link
        if "image" in data:
            data["images"] = [data["image"]]
            del data["image"]
        if "video" in data:
            data["videos"] = [data["video"]]
            del data["video"]
        if "text" in data:
            data["body"] = data["text"]
            del data["text"]
        return self.pack_in_article_container(source=source_map, **data)
    

    def pack_in_data_link_container(self, data: Dict, **kwargs) -> DataLinkContainer:
        source: SourceMap = kwargs["source"]
        if "link" not in data:
            return None
        data_link = urlparse(data['link'])
        if data_link.netloc == "":
            home_url_parse = urlparse(kwargs['url'])
            data['link'] = f"{home_url_parse.scheme}://{home_url_parse.netloc}{data_link.geturl()}"
        if "tags" in data and len(data["tags"]) > 0:
            kwargs["assumed_tags"] = " ".join(data["tags"])
        return DataLinkContainer(container=data,source_name=source.source_name, source_id=source.source_id,
                                 formatter=kwargs['formats'].format_id, scraped_on=datetime.now(),
                                 link=data['link'],assumed_tags=kwargs["assumed_tags"],
                                 compulsory_tags=kwargs['compulsory_tags'], watermarks=source.watermarks,
                                 is_formattable=source.is_structured_aggregator,is_transient=True,
                                 )
    
    def parse_cdata(self, node: Selector, query: Dict):
        cdata_text = self.extract_values(node, parent=query["parent"], param='text()', sel="xpath")
        selected = Selector(text=cdata_text)
        query_copy = query.copy()
        del query_copy["param"]
        del query_copy["parent"]
        return self.extract_values(node=selected, parent=query["param"], param='', param_prefix='', parent_prefix='./', **query_copy)
    
    

    def extract_values(self, node: Selector, parent: str, param: str="text()", parent_prefix=".//", param_prefix="/", **kwargs) -> Union[str, List[str]]:
        f_str = parent_prefix + parent + param_prefix + param
        selected = node.css(f_str) if "sel" in kwargs and kwargs["sel"] == "css" else node.xpath(f_str)
        if "is_multiple" in kwargs and kwargs["is_multiple"]:
            return selected.getall()
        return selected.get()
    
    def parse_format_rules(self, node: Selector, **kwargs):
        collected_data = {}
        for key, value in kwargs["format_rules"].items():
            if key not in self.non_formattable_tags:
                try:
                    if "is_cdata" in value and  value["is_cdata"]:
                        collected_data[key] = self.parse_cdata(node, value)
                    else:
                        collected_data[key] = self.extract_values(node=node, **value)
                except Exception as e:
                    if "testing" in kwargs:
                        print(e)
                    self.error_handling(e)
                    continue
        return collected_data, node
    

    def process_extracted_data(self, data: Dict, node: Selector, **kwargs) -> ParsedNodeValue:
        if "testing" in kwargs and kwargs["testing"]:
            yield data, node
        elif "link_store" in kwargs and kwargs["link_store"].is_multiple:
            self.create_article(data, kwargs["link_store"], kwargs["source_map"],index= kwargs["index"] if "index" in kwargs else 0)
        else:
            yield self.pack_in_data_link_container(data, **kwargs)
    
    # Parse data according format_rules
    def _parse_node(self, response, node: Selector, **kwargs) -> ParsedNodeValue:
        collected_data, node = self.parse_format_rules(node, **kwargs)
        return self.process_extracted_data(collected_data, node, **kwargs)
    
    
    # url must be unique, if it's container of images like pinterest than their src will be url
    def pack_in_article_container(self, link: str, source: SourceMap, title: str="", creator: str ="", images: List[str] = [],
                        disabled: List[str] = [], videos: List[str] = [], text_set: List[str] = [],compulsory_tags: List[str] = [], tags: List[str] = [],
                        body: str= None, index: int=0, pub_date: str = None, content_type = ContentType.Article,
                        scrap_link: str=None) -> ArticleContainer:
                        parsed = urlparse(link)

                        # scrapped url might have possibilites such as
                        # 1. /app/csdgfgkg?dsfg=34 -- when netloc is missing
                        # 2. //google.com/sdfjsfg -- when scheme is missing
                        # 3. everything is fine

                        if parsed.netloc == "":
                            url  = source.source_home_link + parsed.geturl()
                            parsed = urlparse(url)
                        elif parsed.scheme == "":
                            url = "https:"+ parsed.geturl()
                            parsed = urlparse(url)
                        else:
                            url = link


                        return ArticleContainer(
                            article_id=sha256(url.encode()).hexdigest() + str(index),
                            source_id=source.source_id,source_name=source.source_name,
                            title=title,article_link=url,creator=creator,
                            scraped_from=scrap_link,home_link=source.source_home_link,
                            site_name=source.source_name,pub_date=pub_date,
                            disabled=disabled,is_source_present_in_db=True,
                            tags=source.assumed_tags.split(" ") if len(tags) == 0 and source.assumed_tags is not None else tags,
                            compulsory_tags=source.compulsory_tags if len(compulsory_tags) == 0 and source.compulsory_tags is not None else compulsory_tags,
                            images=images,
                            videos=videos,
                            text_set=text_set,
                            body=body,
                            majority_content_type=content_type,
                            next_frame_required=False,
                            scraped_on=datetime.now()
                        )
    
