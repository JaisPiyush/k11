from dataclasses import dataclass, field
from typing import Dict, Generator, List,Optional, Any, Tuple, Union
from datetime import datetime
from datetime import datetime
import json

from sqlalchemy.sql.sqltypes import Enum
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
    


@dataclass
class ContainerIdentity:
    param: str
    is_multiple: Optional[bool] = None

    def to_dict(self, default=False) -> Dict:
        return {
            "param": self.param,
            "is_multiple": self.is_multiple if self.is_multiple is not None else default
        }
    


"""
Container(
    idens = [
        {
            "param": "a",
            "is_multiple": True
        },
        {
            "param": "b",
        }
    ],
    ignorables = [{
        "tag": "a"
    }, {
        "tag": "script"
    },
    
    ]
    terminations = [{
        "tag": "figure"
    }]
    is_multiple = False

)
"""
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
                          QuerySelector(tag="header")
                          ]
    is_multiple:bool = False

    def get_ignorables(self) -> List[str]:
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
    
    def to_dict(self) -> Dict:
        return {
            "idens": [iden.to_dict(default=self.is_multiple) for iden in self.idens],
            "ignorables": [query.to_dict() for query in self.ignorables + self.default_ignorables],
            "terminations": [query.to_dict() for query in self.terminations],
            "is_multiple": self.is_multiple
        }

    def to_json_str(self) -> str:
        return json.dumps(self.to_dict())





@dataclass
class Format(MongoModels):
    __collection_name__ = 'collection_formats'
    __database__ = "mongo_digger"
    source_name: str
    format_id: str   # format_id = source_map.source_id
    source_home_link: str  #source_home_link
    xml_collection_format: Optional[Dict] = None
    html_collection_format: Optional[Dict] = None
    html_article_format: Optional[ContainerFormat] = None
    created_on: datetime = datetime.now()
    extra_formats: Optional[Dict[str, Union[Dict, List[Dict]]]] = None
    
    primary_key: str = 'format_id'

    def get_format(self, format_: str) -> Dict:
        if hasattr(self, format_):
            return self.__getattribute__(format_)
        elif self.extra_formats is not None:
            return self.extra_formats[format_]
        else:
            raise KeyError()
    
    @staticmethod
    def process_kwargs(**kwargs) -> Dict:
        if "html_article_format" in kwargs:
            kwargs["html_article_format"] = ContainerFormat.from_dict(**kwargs["html_article_format"])
        return kwargs
    
    @staticmethod
    def get_default_rss_format():
        return Format.adapter().find_one({"format_id": "default_rss_format"})

"""
Source Map is source links storing format for database
    - source_name: A string compulsory input is the name of source site. 
            for e.g YouTube, Expert Photography, etc.
    - source_id: A string compulsory input which works as unique identifier,
            because existance of different sources from same website is possible.
    - source_home_link: A string compulsory input containing home link to the website.
    - formatter: Key name of formating rules stored inside the source formatter
    - assumed_tags: Each source is associated with some assumed tags, written in definit structure
    - compulsory_tags: These tags are must to be carried unlike assumed tags, 
            they couldn't washed off in any step untill it's overriden
    - links: Array of LinkStore
    - watermarks: Array of possible watermarks in the website content, which must be removed
    - is_structured_aggregator: A boolean which signifies whether we have any respectable article formatter for the content of not,
        if no, then the data_link_container.is_formattable = False, which will force scraper to extract all possible text and rely on webview
"""
@dataclass
class SourceMap(MongoModels):
    __collection_name__ = "collection_source_maps"
    __database__ = "mongo_digger"
    source_name: str 
    source_id: str
    source_home_link: str
    formatter: str
    assumed_tags: str
    compulsory_tags: List[str]
    is_rss: bool
    is_collection: bool
    links: List[LinkStore]
    watermarks: List[str] = field(default_factory=list)
    is_structured_aggregator: bool = True
    datetime_format: str = ""
    primary_key = 'source_id'

    
    @staticmethod
    def process_kwargs(**kwargs):
        if "links" in kwargs:
            kwargs["links"] = [LinkStore.from_dict(**link) for link in kwargs['links']]
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
            "watermarks": self.watermarks,
            "is_structured_aggregator": self.is_structured_aggregator,
            "source_hom_link": self.source_home_link
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
    __database__ = "mongo_digger" 
    source_name: str
    source_id: str
    formatter: str
    link: str
    container: dict
    watermarks: List[str] = field(default_factory=list)
    assumend_tags: Optional[str] = None
    compulsory_tags: Optional[str] = None
    is_formattable: bool = True
    scraped_on: datetime = datetime.now()
    primary_key = "link"

    @staticmethod
    def get_all() -> Generator:
        return DataLinkContainer.adapter().find({})



class ContentType(Enum):
    Article = "article"
    Image = "image"
    Video = "video"

"""
Dataclass for storing all the information of article into mongo(treasury)
"""

@dataclass
class ArticleContainer(MongoModels):
    __collection_name__ = "article_containers"
    __database__ = "mongo_treasure"
    article_id: str
    title: Optional[str]
    creator: Optional[str]
    article_link: Optional[str]
    source_name: Optional[str]
    source_id: Optional[str]
    home_link: str
    site_name: str
    pub_date: Optional[datetime]
    scraped_on: datetime
    text: Optional[str]
    content: Optional[str]
    images: List[str] = field(default_factory=list)
    videos: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    compulsory_tags: List[str] = field(default_factory=list)
    sentiments: int = field(default=0)
    dates: List[str] = field(default_factory=list)
    names: List[str] = field(default_factory=list)
    places: List[str] = field(default_factory=list)
    organisations: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    is_source_present_in_db: bool = False
    majority_content_type: Optional[str] = field(default=ContentType.Article)  # property tell app about content type values can be articel/image/video
    #redirection_required: bool = False
    coords: List[Tuple[float]] = field(default_factory=list)
    primary_key = "article_id"

    
    