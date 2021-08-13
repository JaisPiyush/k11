from datetime import datetime
from mongoengine.document import Document

# from mongoengine.fields import DictField
from .managers import DatalinkContainerQuerySet, FormatsQuerySet, QueuedSourceMapQuerySet, SourceMapQuerySet
from typing import Tuple
from .main import *
import mongoengine as mg



class LinkStoreField(mg.DictField):

    def validate(self, value):
        return isinstance(value, LinkStore) and value.link is not None and len(value.link) > 0
    
    def to_python(self, value):
        if isinstance(value, LinkStore):
            return value
        elif 'link' in value:
            return LinkStore.from_dict(**value)
        return None
    
    def to_mongo(self, value, use_db_field, fields):
        if not isinstance(value, LinkStore):
            return None
        return value.to_dict()


class ContainerFormatField(mg.DictField):

    def validate(self, value):
        return isinstance(value, ContainerFormat)
    
    def to_python(self, value):
        if self.validate(value):
            return value
        else:
            return ContainerFormat.from_dict(**value)
    
    def to_mongo(self, value, use_db_field, fields=None):
        if not self.validate(value):
            return None
        return value.to_dict()


class XMLContainerFormatField(mg.DictField):
    def validate(self, value):
        return isinstance(value, XMLContainerFormat)
    
    def to_python(self, value):
        if self.validate(value):
            return value
        else:
            return XMLContainerFormat.from_dict(**value)
    
    def to_mongo(self, value, use_db_field, fields=None):
        if not self.validate(value):
            return None
        return value.to_dict()


class Format(mg.Document):
    source_name = mg.StringField(required=True)
    format_id = mg.StringField(required=True, unique=True)
    source_home_link = mg.StringField(required=True)
    xml_collection_format = mg.DictField()
    html_collection_format = mg.DictField()
    _html_article_format = mg.DictField(db_field="html_article_format") #ContainerFormatField()
    _xml_article_format = mg.DictField(db_field="xml_article_format") #XMLContainerFormatField()
    # html_article_format = ContainerFormatField()
    # xml_article_format = XMLContainerFormatField()
    created_on = mg.DateTimeField()
    extra_formats = mg.DictField()

    @property
    def html_article_format(self):
        if hasattr(self, "_html_article_format") and getattr(self, "_html_article_format") is not None:
            return ContainerFormat.from_dict(**self._html_article_format)
        return None
    
    @property
    def xml_article_format(self):
        if hasattr(self, "_xml_article_format") and getattr(self, "_xml_article_format") is not None:
            return XMLContainerFormat.from_dict(**self._xml_article_format)
        return None

    meta = {
        "db_alias": "mongo_digger",
        "collection": "collection_formats",
        "queryset_class": FormatsQuerySet,
    }

 
class SourceMap(mg.Document):
    source_name = mg.StringField(required=True)
    source_id = mg.StringField(required=True, unique=True)
    source_home_link = mg.StringField(required=True)
    assumed_tags = mg.StringField()
    compulsory_tags = mg.ListField(mg.StringField())
    is_rss = mg.BooleanField(default=True)
    is_collection = mg.BooleanField(default=True)
    links = mg.ListField(LinkStoreField())
    formatter = mg.StringField()
    watermarks = mg.ListField(mg.StringField())
    is_structured_aggregator = mg.BooleanField(default=True)
    datetime_format = mg.StringField()
    is_third_party = mg.BooleanField(default=False)
    source_locations = mg.ListField(mg.StringField())

    meta = {
        "db_alias": "mongo_digger",
        "collection": "collection_source_maps",
        'queryset_class': SourceMapQuerySet,
    }

    def get_tags(self, li: str) -> Tuple[str, str]:
        for link in self.links:
            if link.link == li:
                return link.assumed_tags if link.assumed_tags != None else self.assumed_tags, link.compulsory_tags if link.compulsory_tags != None else self.compulsory_tags



class QueuedSourceMap(Document):

    source_name = mg.StringField(required=True)
    source_id = mg.StringField(required=True, unique=True)
    source_home_link = mg.StringField(required=True)
    assumed_tags = mg.StringField()
    compulsory_tags = mg.ListField(mg.StringField())
    is_rss = mg.BooleanField(default=True)
    is_collection = mg.BooleanField(default=True)
    links = mg.ListField(LinkStoreField())
    formatter = mg.StringField()
    watermarks = mg.ListField(mg.StringField())
    is_structured_aggregator = mg.BooleanField(default=True)
    datetime_format = mg.StringField()
    is_third_party = mg.BooleanField(default=False)

    meta = {
        "db_alias": "mongo_digger",
        "collection": "collection_queued_source_maps",
        'queryset_class': QueuedSourceMapQuerySet,
        "allow_inheritance": True
    }


    @classmethod
    def from_source_map(cls, source_map: SourceMap):
        data = {key: getattr(source_map, key) for key in source_map._db_field_map.keys() if key != "_cls"}
        return cls(**data)

    def get_tags(self, li: str) -> Tuple[str, str]:
        for link in self.links:
            if link.link == li:
                return link.assumed_tags if link.assumed_tags != None else self.assumed_tags, link.compulsory_tags if link.compulsory_tags != None else self.compulsory_tags


class DataLinkContainer(mg.Document):
    source_name = mg.StringField(required=True)
    source_id = mg.StringField(required=True)
    formatter = mg.StringField(required=True)
    link = mg.StringField(required=True)
    container = mg.DictField()
    watermarks = mg.ListField(mg.StringField())
    assumed_tags = mg.StringField()
    compulsory_tags = mg.StringField()
    is_formattable = mg.BooleanField(default=True)
    is_transient = mg.BooleanField(default=True)
    scraped_on = mg.DateTimeField(default=datetime.now())

    meta = {
        'db_alias': 'mongo_digger',
        'collection': 'data_link_containers',
        "queryset_class": DatalinkContainerQuerySet
    }


    



class ArticleContainer(mg.Document):
    article_id = mg.StringField(required=True)
    title = mg.StringField(required=False)
    creator = mg.StringField(required=False)
    article_link = mg.StringField(required=True)
    source_name = mg.StringField(required=True)
    source_id = mg.StringField(required=True)
    scraped_from = mg.StringField(required=True)
    home_link = mg.StringField(required=True)
    site_name = mg.StringField()
    pub_date = mg.DateTimeField(default=datetime.now())
    scraped_on = mg.DateTimeField(default=datetime.now()) 
    text_set = mg.ListField(mg.StringField())
    body = mg.StringField()
    disabled = mg.ListField(mg.StringField())
    images = mg.ListField(mg.StringField())
    videos = mg.ListField(mg.StringField())
    tags = mg.ListField(mg.StringField())
    compulsory_tags = mg.ListField(mg.StringField())
    dates = mg.ListField(mg.StringField())
    names = mg.ListField(mg.StringField())
    places = mg.ListField(mg.StringField())
    organizations = mg.ListField(mg.StringField())
    keywords = mg.ListField(mg.StringField())
    next_frame_required = mg.BooleanField(default=True)
    is_source_present_in_db = mg.BooleanField(default=False)
    majority_content_type = mg.StringField()
    is_discriminated = mg.BooleanField(default=False)
    discriminated_on = mg.DateTimeField(default=None)
    coords = mg.ListField(mg.ListField(mg.FloatField()))
    meta_data = mg.DictField()


    meta = {
        "db_alias":"mongo_treasure",
        "collection": "article_containers"
    }





class ArticleGrave(mg.Document):
    title = mg.StringField(required=True)
    creator = mg.StringField(required=True)
    article_link = mg.StringField(required=True)
    source_name = mg.StringField(required=True)
    source_id = mg.StringField(required=True)
    scraped_from = mg.StringField(required=True)
    home_link = mg.StringField(required=True)
    site_name = mg.StringField()
    pub_date = mg.DateTimeField(default=datetime.now())
    scraped_on = mg.DateTimeField(default=datetime.now())
    text_set = mg.ListField(mg.StringField())
    body = mg.StringField()
    tags = mg.ListField(mg.StringField())
    compulsory_tags = mg.ListField(mg.StringField())
    dates = mg.ListField(mg.StringField())
    names = mg.ListField(mg.StringField())
    places = mg.ListField(mg.StringField())
    organizations = mg.ListField(mg.StringField())
    keywords = mg.ListField(mg.StringField())
    coords = mg.ListField(mg.ListField(mg.FloatField()))
    meta_data = mg.DictField()

    meta = {
        "db_alias": "mongo_grave",
        "collection": "article_grave"
    }


class ErrorLogs(mg.Document):
    time = mg.DateTimeField(default=datetime.now())
    level = mg.IntField(required=True)
    message = mg.StringField(required=True)
    meta = {
        "db_alias": "mongo_admin",
        "collection": "error_logs"
    }

# connection_handler.mount_mongo_engines()