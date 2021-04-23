from dataclasses import dataclass, field
from typing import Dict, List,Optional, Any, Union
from datetime import datetime
from datetime import datetime
from hashlib import sha256
from .mongo import MongoModels

@dataclass
class Selection:
    param: str
    sel: str = 'xpath'
    parent: str = None
    type: Optional[str] = None

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
    is_dynamic: Optional[bool] = False
    def to_dict(self):
        return {
            "link": self.link,
            "assumed_section": self.assumed_tags,
            "formatter": self.formatter,
            "is_dynamic": self.is_dynamic,
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
    format_id: str = sha256(source_name+'_formats').hexdigest()
    primary_key: str = 'format_id'

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
    formatter: str
    assumed_tags: str
    compulsory_tags: List[str]
    is_rss: bool
    is_collection: bool
    links: List[LinkStore]
    watermarks: List[str] = field(default_factory=list)
    source_id: str = sha256(source_name).digest()
    primary_key: str = 'source_id'
    
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
    
    @staticmethod
    def pull_all_rss_models():
        yield SourceMap.adapter().find({"$and": [{"is_rss": True}, {"is_collection": True}]})

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
    watermarks: List[str] = []
    primary_key = "link"

    def get_collection_name(self) -> str:
        return self.__collection_name__

    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)


