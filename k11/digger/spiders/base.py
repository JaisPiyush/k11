from datetime import datetime
import logging
import re
from k11.models.no_sql_models import QueuedSourceMap
from k11.digger.abstracts import BaseSpider
# import traceback
# from k11.vault import connection_handler

from scrapy.http.request import Request
from k11.utils import is_url_valid
from k11.digger.abstracts import AbstractCollectionScraper, ParsedNodeValue
from typing import Dict,List, Tuple, Union
from k11.models.serializer import DataLinkSerializer
from scrapy import Selector
from urllib.parse import urlparse
from scrapy_splash.request import SplashRequest
from k11.models.models import Format, LinkStore, SourceMap, DataLinkContainer, ContentType, ArticleContainer
from urllib.parse import urlparse
from hashlib import sha256
import random


class BaseCollectionScraper(AbstractCollectionScraper):

    default_format_rules = None
    scraped_sources_count = 0
    sql_session = None

    """
    Every Link Store is associated with a formatter name such as `xml_collection_format` or `html_collection_format`
    """
    def get_suitable_format_rules(self, formats: Format, source_map: SourceMap, link_store: LinkStore, default="") -> Dict:
        if link_store.formatter != None and len(link_store.formatter) > 0:
            if link_store.formatter != source_map.formatter and not hasattr(formats,link_store.formatter):
                return formats.extra_formats[link_store.formatter]
            default = link_store.formatter
        elif source_map.formatter is not None and len(source_map.formatter) > 0:
            default = source_map.formatter
        return getattr(formats, default)
     
    
    def get_tags_for_link_store(self, source_map: SourceMap, link_store: LinkStore) -> Tuple[str, List[str]]:
        assumed_tags, compulsory_tags = "", []
        if link_store.assumed_tags != None and len(link_store.assumed_tags) > 0:
            assumed_tags = link_store.assumed_tags
        else:
            assumed_tags = source_map.assumed_tags
        if link_store.compulsory_tags != None and len(link_store.compulsory_tags) > 0:
            compulsory_tags = link_store.compulsory_tags
        else:
            compulsory_tags = link_store.compulsory_tags
        return assumed_tags, compulsory_tags

    def call_request(self, url: str, source: SourceMap, format_rules: Dict, formats: Format, assumed_tags: str, compulsory_tags: List[str],
                            splash_headers={'User-Agent': "Mozilla/5.0 (Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"}, link_store: LinkStore = None, **kwargs):
                            if "cb_kwargs" not in kwargs:
                                kwargs['cb_kwargs'] = {}
                            kwargs['cb_kwargs'].update({
                                "source_map": source,
                                "format_rules": format_rules,
                                "formats": formats,
                                "assumed_tags": assumed_tags,
                                "compulsory_tags": compulsory_tags,
                                "url": url,
                                "link_store": link_store
                            })
                            return SplashRequest(url=url, args={"wait": 1.8}, splash_headers=splash_headers, **kwargs)

    
    def process_link_store(self, link_store: LinkStore, source_map: SourceMap, formats: Format, **kwargs) -> Request:
        format_rules = self.get_suitable_format_rules(formats=formats, source_map=source_map, link_store=link_store,default=self.default_format_rules)
        
        assumed_tags, compulsory_tags = self.get_tags_for_link_store(source_map=source_map, link_store=link_store)

        data = self.before_requesting(url=link_store.link, callback=self._parse, formats=formats, format_rules=format_rules, source=source_map, 
        assumed_tags=assumed_tags, compulsory_tags=compulsory_tags, link_store=link_store)

        # Merging transformed data with all of the arguments to create a data packet
        if "cb_kwargs" not in data:
            data["cb_kwargs"] = {}

        # Defining iterator type for collection scrapper, html/xml
        if link_store.formatter == "html_collection_format":
            data['cb_kwargs']['itertype'] = "html"
        elif link_store.formatter == "xml_collection_format":
            data['cb_kwargs']['itertype'] = "xml"
        data["cb_kwargs"].update(kwargs)
        return self.call_request(**data)

    def run_requests(self, **kwargs):
        sources = list(self.get_sources_from_database())
        random.shuffle(sources)
        for source in sources:
            print(source.source_name, " ", source.source_home_link, "\n")
            formats = self.get_formatter_from_database(source.source_id)
            if formats == None:
                continue
            for link_store in source.links:
                # print(link_store, link_store.link != None and len(link_store.link) > 0 and is_url_valid(link_store.link))
                if link_store.link != None and len(link_store.link) > 0 and is_url_valid(link_store.link):
                    try:
                        yield self.process_link_store(link_store, source, formats, **kwargs)
                    except Exception as e:
                        self.log(f"Error from run requests {kwargs} with error {e} on link {link_store.link} ")
                else:
                    continue
            QueuedSourceMap.objects(source_id=source.source_id).delete()
            self.scraped_sources_count += 1
    
    def start_requests(self, **kwargs):
        self.scraped_sources_count = 0
        return self.run_requests(**kwargs)
        


class ScrapedValueProcessor:


    @staticmethod
    def run_post_function(operation, kwargs: dict):
        if operation == "replace":
            kwargs["pattern"] = re.compile(kwargs["pattern"])
            return re.sub(**kwargs)
    
    @staticmethod
    def get_kwarg_formatter(kwargs: dict, injectable_value) -> Dict:
        rt_kwargs = kwargs.copy()
        for key, value in kwargs.items():
            if value == "self":
                rt_kwargs[key] = injectable_value
        return rt_kwargs
    
    @staticmethod
    def apply_function_on_value(func, value):
        if isinstance(value, list) or isinstance(value, set):
            return list(filter(lambda val: val is not None and len(val) > 0 ,map(func, value)))
        elif isinstance(value , dict):
            return {key: func(val) for key,val in value.items() if val is not None }
        return func(value)

    """
    Function will remove any restrictive parameter present in url
    for e.g in Pedron The World urls have `url?format=300w`, which 
    defines the size of url, i.e needed to be removed
    """
    def process_extracted_data(self, data: dict, post_functions) -> List[str]:
        # print(post_functions)
        if post_functions is not None:
            for key, value in post_functions.items():
                if key in data:
                    function = lambda arg: self.run_post_function(value['op'],self.get_kwarg_formatter(value["params"], arg))
                    data[key] = self.apply_function_on_value(func=function, value=data[key])
        return data









class BaseContentExtraction(BaseSpider):

    non_formattable_tags = ['itertag', 'namespaces', "post_functions"]

    """
    Handles different types of error during parsing
    """

    def error_handling(self, e):
        self.log(e, level=logging.ERROR)

    def create_article(self, data: Dict, link_store: LinkStore, source_map: SourceMap, index: int = 0, post_functions=None):
        try:
            if ArticleContainer.objects(article_link = data["link"]).count() == 0 and data['link'] is not None:
                article: ArticleContainer = self.process_single_article_data(data=data, link_store=link_store, source_map=source_map, index=index, post_functions=post_functions)
                article.save()
        except KeyError as e:
            self.log(f"`BaseContentExtraction.create_article` is throwing {e} with data {data} for {source_map.source_name} on link {link_store.link}", level=logging.ERROR)

    def process_single_article_data(self, data: Dict, link_store: LinkStore, source_map: SourceMap, index : int = 0, post_functions=None):
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
        # filter None values
        if "images" in data:
            data["images"] = set(filter(lambda x: x is not None, data["images"]))
        if "videos" in data:
            data["videos"] = set(filter(lambda x: x is not None, data["videos"]))
        if post_functions is not None:
            scp = ScrapedValueProcessor()
            data = scp.process_extracted_data(data, post_functions=post_functions)
        return self.pack_in_article_container(source=source_map, **data)
    

    def pack_in_data_link_container(self, data: Dict, **kwargs) -> DataLinkContainer:
        source: SourceMap = kwargs["source_map"]
        if "link" not in data:
            return None
        data_link = urlparse(data['link'])
        if data_link.netloc == "":
            home_url_parse = urlparse(kwargs['url'])
            data['link'] = f"{home_url_parse.scheme}://{home_url_parse.netloc}{data_link.geturl()}"
        return DataLinkSerializer(DataLinkContainer(container=data,source_name=source.source_name, source_id=source.source_id,
                                 formatter=kwargs['formats'].format_id, scraped_on=datetime.now(),
                                 link=data['link'],assumed_tags=kwargs["assumed_tags"],
                                 compulsory_tags=kwargs['compulsory_tags'], watermarks=source.watermarks,
                                 is_formattable=source.is_structured_aggregator,is_transient=True,
                                 ))
    
    def parse_cdata(self, node: Selector, query: Dict):

        # TODO: Needs to update creator format rules in database
        # self.log(f'node={node}, parent={query["parent"]}, n={node.xpath(".//dc:creator//p/text()").get()}')
        
        if "creator" in query["parent"]:
            if ":" not in query["parent"]:
                query['parent'] = "dc:"+query["parent"]
            if query["param"] == "/p/text()":
                query['param'] = "text()"
            return self.extract_values(node,**query)
        cdata_text = self.extract_values(node, parent=query["parent"], param='text()', sel="xpath")
        
        selected = Selector(text=cdata_text)
        query_copy = query.copy()
        del query_copy["param"]
        del query_copy["parent"]
        # self.log(self.extract_values(node=selected, parent=query["param"], param='', param_prefix='', parent_prefix='./', **query_copy), only_screen=True)
        return self.extract_values(node=selected, parent=query["param"], param='', param_prefix='', parent_prefix='./', **query_copy)
        
    
    

    def extract_values(self, node: Selector, parent: str, param: str="text()", parent_prefix=".//", param_prefix="/", **kwargs) -> Union[str, List[str]]:
        # {parent: "__", param: "abv"} will delete the '//' from parent_prefix
        if parent == "__":
            parent_prefix = "."
            parent = ""
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
            # is_multiple signifies that the articles are directly baked into collection
            self.create_article(data, kwargs["link_store"], kwargs["source_map"],index= kwargs["index"] if "index" in kwargs else 0, post_functions=kwargs["format_rules"].get("post_functions", None))
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
                        scrap_link: str=None, **kwargs) -> ArticleContainer:
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
    
