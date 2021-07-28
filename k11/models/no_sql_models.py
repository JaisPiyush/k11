from datetime import datetime

from mongoengine.fields import BooleanField, DictField
from .managers import DatalinkContainerQuerySet, FormatsQuerySet, SourceMapQuerySet
from typing import Tuple
from .main import ContainerFormat, LinkStore, XMLContainerFormat
import mongoengine as mg
from k11.vault.app import connection_handler
from mongoengine.queryset.visitor import Q

from scrapy.item import Item


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


class ContainerFormatField(DictField):

    def validate(self, value):
        return isinstance(value, ContainerFormat)
    
    def to_python(self, value):
        if self.validate(value):
            return value
        else:
            return ContainerFormat.from_dict(**value)
    
    def to_mongo(self, value, use_db_field, fields):
        if not self.validate(value):
            return None
        return value.to_dict()


class XMLContainerFormatField(DictField):
    def validate(self, value):
        return isinstance(value, XMLContainerFormat)
    
    def to_python(self, value):
        if self.validate(value):
            return value
        else:
            return XMLContainerFormat.from_dict(**value)
    
    def to_mongo(self, value, use_db_field, fields):
        if not self.validate(value):
            return None
        return value.to_dict()


class Format(mg.Document):
    source_name = mg.StringField(required=True)
    format_id = mg.StringField(required=True, unique=True)
    source_home_link = mg.StringField(required=True)
    xml_collection_format = mg.DictField()
    html_collection_format = mg.DictField()
    html_article_format = ContainerFormatField()
    xml_article_format = XMLContainerFormatField()
    created_on = mg.DateTimeField()
    extra_formats = mg.DictField()

    meta = {
        "db_alias": "mongo_digger",
        "collection": "collection_formats",
        "queryset_class": FormatsQuerySet
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

    meta = {
        "db_alias": "mongo_digger",
        "collection": "collection_source_maps",
        'queryset_class': SourceMapQuerySet
    }

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