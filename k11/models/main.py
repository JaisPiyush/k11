from dataclasses import dataclass, field
from ._serializer import InterfaceSerializer
from typing import Dict, List,Optional
import json

from mongoengine.fields import DictField


class ContentType:
    Article = "article"
    Image = "image"
    Video = "video"

@dataclass
class Selection:
    param: str
    sel: str = 'xpath'
    parent: str = None
    type: Optional[str] = "text"
    is_multiple: Optional[bool] = False
    is_cdata: Optional[bool] = False

    def to_dict(self):
        return {
            "sel": self.sel,
            "param": self.param,
            "type": self.type,
            "parent": self.parent,
            "is_multiple": self.is_multiple,
            "is_cdata": self.is_cdata
        }
    
    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)


@dataclass
class LinkStore(DictField):
    link: str
    assumed_tags: Optional[str] = None
    formatter: Optional[str] = None
    compulsory_tags: Optional[str] = None
    content_type: Optional[str] = ContentType.Article
    is_multiple: Optional[bool] = False

    def to_dict(self):
        return {
            "link": self.link,
            "assumed_tags": self.assumed_tags,
            "compulsory_tags": self.compulsory_tags,
            "formatter": self.formatter,
            "content_type": self.content_type,
            "is_multiple": self.is_multiple
        }
    
    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)


@dataclass
class QuerySelector:
    tag: Optional[str] = None
    id: Optional[str] = None
    class_list: Optional[List[str]] = None
    exact_class: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "tag": self.tag,
            "id": self.id,
            "class_list": self.class_list,
            "exact_class": self.exact_class
        }
    
    def __getitem__(self, key):
        return getattr(self, key)
    
    
    


@dataclass
class ContainerIdentity:
    param: str
    is_multiple: Optional[bool] = None
    content_type: Optional[str] = field(default=ContentType.Article)
    is_bakeable: Optional[bool] = False

    def to_dict(self, default=False) -> Dict:
        return {
            "param": self.param,
            "is_multiple": self.is_multiple if self.is_multiple is not None else default,
            "content_type": self.content_type,
            "is_bakeable": self.is_bakeable
        }
    
    def to_json_str(self):
        return json.dumps(self.to_dict())
    
    def __eq__(self, o: object) -> bool:
        return (hasattr(o, "param") and getattr(o, "param") == self.param and
                hasattr(o, "is_multiple") and getattr(o, "is_multiple") == self.is_multiple and
                hasattr(o, "content_type") and getattr(o, "content_type") == self.content_type and
                hasattr(o, "is_bakeable") and getattr(o,"is_bakeable" ) == self.is_bakeable
                  )
    
    def __getitem__(self, key):
        return getattr(self, key)
    


@dataclass
class ContainerFormat:
    idens: List[ContainerIdentity]
    ignorables: List[QuerySelector] = field(default_factory=list)
    terminations: List[QuerySelector] = field(default_factory=list)
    default_ignorables = [QuerySelector(tag="script"), 
                          QuerySelector(tag="noscript"),
                          QuerySelector(tag="style"),
                          QuerySelector(tag="input"),
                          QuerySelector(tag="footer"),
                          QuerySelector(tag="form"),
                          QuerySelector(tag="header"),
                          QuerySelector(tag="textarea")
                          ]
    is_multiple:bool = False
    title_selectors: Optional[List[str]] = None
    creator_selectors: Optional[List[str]] = None
    body_selectors: Optional[List[str]] = None
    # {key: {op: "replace", params: {param_: value}}}
    post_functions: Optional[Dict] = None

    


    def get_ignoreables(self) -> List[str]:
        return self.default_ignorables + self.ignorables
    
    @classmethod
    def from_dict(cls, **kwargs):
        for index, iden in enumerate(kwargs['idens']):
            kwargs['idens'][index] = ContainerIdentity(**iden)
        
        if "ignorables" in kwargs:
            for index, query in enumerate(kwargs['ignorables']):
                kwargs['ignorables'][index] = QuerySelector(**query)
        if "terminations" in kwargs:
            for index, query in enumerate(kwargs['terminations']):
                kwargs['terminations'][index] = QuerySelector(**query)
        
        return cls(**kwargs)
    
    @staticmethod
    def from_dict_to_json(**kwargs) -> str:
        model  = ContainerFormat.from_dict(**kwargs)
        return model.to_json_str()
    
    def to_dict(self) -> Dict:
        # return {
        #     "idens": [iden.to_dict(default=self.is_multiple) for iden in self.idens],
        #     "ignorables": [query.to_dict() for query in self.ignorables + self.default_ignorables],
        #     "terminations": [query.to_dict() for query in self.terminations],
        #     "is_multiple": self.is_multiple,
        #     "title_selectors": self.title_selectors,
        #     "creator_selectors": self.creator_selectors,
        #     "body_selectors": self.body_selectors,
        #     "post_functions": self.post_functions
        # }
        return InterfaceSerializer(self)
    
    @property
    def get_idens_dict(self):
        return [iden.to_dict(default=self.is_multiple) for iden in self.idens]
    
    @property
    def get_ignorables_dict(self):
        return [query.to_dict() for query in self.ignorables]

    def to_json_str(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class XMLContainerFormat:
    struct: Optional[Dict] = field(default=None)
    content_type: Optional[str] = field(default=None)

    def to_dict(self) -> Dict:
        data = {}
        if self.struct is not None:
            data["struct"] = self.struct
        if self.content_type is not None:
            data["content_type"] = self.content_type
        return data
    
    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)



"""
ThirdPartyDigger contains all non scrapy spiders.
The run(self,**kwargs) function will be called by our PilotClass
"""
class ThirdPartyDigger:

    def run(self, **kwargs): ...
