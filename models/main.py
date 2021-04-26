from dataclasses import dataclass, field
from typing import Dict, Generator, List,Optional, Any, Tuple, Union
from datetime import datetime
from datetime import datetime
from hashlib import sha256
from .mongo import MongoModels

@dataclass
class Selection:
    param: str
    sel: str = 'xpath'
    parent: str = None
    type: Optional[str] = "text"

    def to_dict(self):
        return {
            "sel": self.sel,
            "param": self.param,
            "type": self.type,
            "parent": self.parent
        }
    
    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)


@dataclass
class LinkStore:
    link: str
    assumed_tags: Optional[str] = None
    formatter: Optional[str] = None
    compulsory_tags: Optional[str] = None
    def to_dict(self):
        return {
            "link": self.link,
            "assumed_section": self.assumed_tags,
            "compulsory_tags": self.compulsory_tags,
            "formatter": self.formatter,
        }
    
    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)





@dataclass
class Format(MongoModels):
    __collection_name__ = 'collection_formats'
    __database__ = "digger"
    source_name: str
    xml_collection_format: Optional[Dict] = None
    html_collection_format: Optional[Dict] = None
    html_article_format: Optional[Dict] = None
    created_on: datetime = datetime.now()
    extra_formats: Optional[Dict[str, List[Dict]]] = None
    format_id = None   # format_id = source_map.source_id
    primary_key: str = 'format_id'

    def __post_init__(self) -> None:
        self.format_id = sha256(self.source_name+'_formats').hexdigest()

    def get_format(self, format_: str) -> Dict:
        if hasattr(self, format_):
            return self.__getattribute__(format_)
        elif self.extra_formats is not None:
            return self.extra_formats[format_]
        else:
            raise KeyError()
    
    @staticmethod
    def get_default_rss_format():
        return Format.adapter().find_one({"format_id": "default_rss_format"})

@dataclass
class SourceMap(MongoModels):
    __collection_name__ = "collection_source_maps"
    __database__ = "digger"
    source_name: str
    source_id: str
    formatter: str
    assumed_tags: str
    compulsory_tags: List[str]
    is_rss: bool
    is_collection: bool
    links: List[LinkStore]
    watermarks: List[str] = field(default_factory=list)
    # source_id: str = sha256(source_name).digest()
    primary_key: str = 'source_id'

    def process_kwargs(self, **kwargs):
        if "links" in kwargs:
            kwargs["links"] = [LinkStore.from_dict(link) for link in kwargs['links']]
        return kwargs
    
    # Overriding default `to_dict` method
    def to_dict(self) -> Dict:
        return  {
            "source_name": self.source_name,
            "source_id": self.source_id,
            "formatter": self.formatter,
            "assumed_tags": self.assumed_tags,
            "is_rss": self.is_rss,
            "is_collection": self.is_collection,
            "links": [link.to_dict() for link in self.links],
            "compulsory_tags": self.compulsory_tags,
            "watermarks": self.watermarks
        }
    
    def get_tags(self, li: str) -> Tuple[str, str]:
        for link in self.links:
            if link.link == li:
                return link.assumed_tags if link.assumed_tags != None else self.assumed_tags, link.compulsory_tags if link.compulsory_tags != None else self.compulsory_tags

    
    @staticmethod
    def pull_all_rss_models() -> Generator:
        return SourceMap.adapter().find({"$and": [{"is_rss": True}, {"is_collection": True}]})
    
    @staticmethod
    def pull_all_html_collections() -> Generator:
        return SourceMap.adapter().find({"$and": [{"is_rss": False}, {"is_collection":True}]})



@dataclass
class DataLinkContainer(MongoModels):
    __collection_name__ = "data_link_containers"
    __database__ = "digger"
    container: Dict
    source_name: str
    source_id: str
    formatter: str
    scraped_on: datetime
    link: str = None
    watermarks: List[str] = field(default_factory=list)
    assumend_tags: Optional[str] = None
    compulsory_tags: Optional[str] = None
    primary_key = "link"



