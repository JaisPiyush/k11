
from datetime import datetime
from vault.adapter import TableAdapter
from sqlalchemy import Table, Column, DateTime, MetaData, String
from sqlalchemy.ext.declarative import declarative_base

   
Base = declarative_base()

class IndexableLinks(Base):
    __tablename__ = "indexable_links"
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
    