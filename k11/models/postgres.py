
from datetime import date, datetime

from sqlalchemy.sql.sqltypes import Date
from k11.vault.adapter import TableAdapter
from sqlalchemy import Table, Column, DateTime, MetaData, String
from sqlalchemy.ext.declarative import declarative_base

   
Base = declarative_base()

class IndexableLinks(Base):
    __tablename__ = "indexable_links"
    __database__ = "postgres_digger"
    link = Column(String(2000), primary_key=True, index=True)
    scraped_on = Column(DateTime, default=datetime.now())
    source_name = Column(String(50))
    primary_key = "link"

    @classmethod
    def adapter(cls) -> TableAdapter:
        return TableAdapter(cls)
    
    @classmethod
    def get_primary_key(cls) -> Column:
        return getattr(cls, cls.primary_key)
    
    def create(self):
        self.adapter()._instance_create(self)


class IndexableArticle(Base):
    __tablename__ = "article_indexable_link"
    __database__ = "postgres_treasure"
    article_id = Column(String(260), primary_key=True, index=True)
    title = Column(String(300))
    creator = Column(String(70))
    site_name = Column(String(70))
    scraped_on = Column(DateTime, default=datetime.now())
    pub_date = Column(DateTime, default=datetime.now())
    

    @classmethod
    def adapter(cls) -> TableAdapter:
        return TableAdapter(cls)
    
    @classmethod
    def get_primary_key(cls) -> Column:
        return getattr(cls, cls.primary_key)
    
    def create(self):
        self.adapter()._instance_create(self)
