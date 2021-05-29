from datetime import datetime
from typing import Dict, List, Tuple
from scrapy.spiders import Spider
from urllib.parse import urlparse
from scrapy_splash.request import SplashRequest
from k11.models.main import Format, LinkStore, SourceMap, DataLinkContainer

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
                            splash_headers={'User-Agent': "Mozilla/5.0 (Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"}, **kwargs):
                            if "cb_kwargs" not in kwargs:
                                kwargs['cb_kwargs'] = {}
                            kwargs['cb_kwargs'].update({
                                "source": source,
                                "format_rules": format_rules,
                                "formats": formats,
                                "assumed_tags": assumed_tags,
                                "compulsory_tags": compulsory_tags,
                                "url": url,
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