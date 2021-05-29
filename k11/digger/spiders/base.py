from datetime import datetime
from typing import Dict, List, Tuple
from scrapy.spiders import Spider
from urllib.parse import urlparse
from scrapy_splash.request import SplashRequest
from k11.models.main import Format, LinkStore, SourceMap, DataLinkContainer, ContentType, ArticleContainer
from urllib.parse import urlparse
from hashlib import sha256

class BaseCollectionScraper(Spider):
    
    """
        This function will decide which formatting rules to be used for the current link,
        if link_store contains any formatter and the formatter is not equal to default formatter of source
        i.e html_collection_format/xml_collection_format

    """

    def get_suitable_format_rules(self, formats: Format, source: SourceMap, link_store: LinkStore, default="") -> Dict:
        format_rules = None
        if link_store.formatter != None and len(link_store.formatter) > 0 and link_store.formatter != source.formatter and link_store.formatter in formats.extra_formats:
            format_rules = formats.extra_formats[link_store.formatter]
        else:
            format_rules =  getattr(formats, default)
        # if "namespaces" in format_rules:
        #     self.namespaces = list(self.namespaces) +  format_rules['namespaces']
        return format_rules
    

    def get_tags(self, source: SourceMap, link_store: LinkStore) -> Tuple[str, List[str]]:
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

    
    def pack_data_in_container(self, data: Dict, **kwargs) -> DataLinkContainer:
        source: SourceMap = kwargs["source"]
        if "link" not in data:
            return None
        data_link = urlparse(data['link'])
        if data_link.netloc == "":
            home_url_parse = urlparse(kwargs['url'])
            data['link'] = f"{home_url_parse.scheme}://{home_url_parse.netloc}{data_link.geturl()}"
        return DataLinkContainer(container=data,
                                 source_name=source.source_name, source_id=source.source_id,
                                 formatter=kwargs['formats'].format_id, scraped_on=datetime.now(),
                                 link=data['link'],
                                 assumed_tags=kwargs["assumed_tags"],
                                 compulsory_tags=kwargs['compulsory_tags'], watermarks=source.watermarks,
                                 is_formattable=source.is_structured_aggregator,
                                 is_transient=True,
                                 )


class MultiplContentFromCollection:

    def create_data(self, data: Dict, link_store: LinkStore, source_map: SourceMap, index: int = 0):
        if ArticleContainer.adapter().find_one({"article_link" : data["link"]}, silent=True) == None:
            ArticleContainer.adapter().create(**self.process_single_data(data=data, link_store=link_store, source_map=source_map, index=index))

    def process_single_data(self, data: Dict, link_store: LinkStore, source_map: SourceMap, index : int = 0):
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
        return self.pack_container(source=source_map, **data)
        

    
    # url must be unique, if it's container of images like pinterest than their src will be url
    def pack_container(self, link: str, source: SourceMap, title: str="", creator: str ="", images: List[str] = [],
                        disabled: List[str] = [], videos: List[str] = [], text_set: List[str] = [],compulsory_tags: List[str] = [], tags: List[str] = [],
                        body: str= None, index: int=0, pub_date: str = None, content_type = ContentType.Article,
                        scrap_link: str=None):
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
                            source_id=source.source_id,
                            source_name=source.source_name,
                            title=title,
                            article_link=url,
                            creator=creator,
                            scraped_from=scrap_link,
                            home_link=source.source_home_link,
                            site_name=source.source_name,
                            pub_date=pub_date,
                            disabled=disabled,
                            is_source_present_in_db=True,
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
