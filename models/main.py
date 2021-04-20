from dataclasses import dataclass
from typing import Dict, List,Optional, Any
from datetime import datetime
from datetime import datetime
import json
from bson.objectid import ObjectId


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
class CollectionFormatStructure:
    collection: Dict[str, Selection] = {}
    def __getitem__(self, key):
        return self.collection[key]

    def __delattr__(self, name: str) -> None:
        del self.collection[name]

    def __setattr__(self, name: str, value: Any) -> None:
        self.collection[name] = value
    
    def __getattribute__(self, name: str) -> Any:
        return self.collection[name]

    def to_dict(self):
        return  {key: value.to_dict() for key, value in self.collection.items()} if self.collection != None else None,
            
        
    
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
class Format:
    source_name: str
    _id: Optional[ObjectId]
    xml_collection_format: Optional[Dict] = None
    html_collection_format: Optional[Dict] = None
    html_article_format: Optional[Dict] = None
    created_on: datetime = datetime.now()

    def to_dict(self) -> Dict:
        return {
            "source_name": self.source_name,
            "_id": self._id,
            "xml_collection_format": self.xml_collection_format,
            "html_collection_format": self.html_collection_format,
            "html_article_format": self.html_article_format,
            "created_on": self.created_on
        }
    
    def to_json(self):
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)


@dataclass
class SourceMap:
    source_name: str
    _id: Optional[ObjectId]
    source_id: str
    formatter: str
    assumed_tags: str
    compulsory_tags: List[str]
    is_rss: bool
    is_collection: bool
    links: List[LinkStore]

    def to_dict(self) -> Dict:
        return {
            "source_name": self.source_name,
            "source_id": self.source_id,
            "_id": self._id,
            "formatter": self.formatter,
            "assumed_tags": self.assumed_tags,
            "is_rss": self.is_rss,
            "is_collection": self.is_collection,
            "links": [link.to_dict() for link in self.links],
            "compulsory_tags": self.compulsory_tags
        }
    
    def to_json(self):
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)


@dataclass
class DataLinkContainer:
    __collection_name__ = "data_link_containers"
    container: Dict
    source_name: str
    source_id: str
    formatter: str
    scraped_on: datetime
    link: str

    def get_collection_name(self) -> str:
        return self.__collection_name__

    def to_dict(self) -> Dict:
        return {
            "container": self.container,
            "source_name": self.source_name,
            "source_id": self.source_id,
            "formatter": self.formatter,
            "scraped_on": self.scraped_on,
            "link": self.link
        }
    
    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)


