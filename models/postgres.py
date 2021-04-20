from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy import Table, Column, DateTime, MetaData, String
from sqlalchemy.engine.base import Engine


@dataclass
class IndexableLink:
    __collection_name__ = "indexable_links"
    link: str
    scraped_on: Optional[datetime] = datetime.now()

    @staticmethod
    def to_table(meta_data: MetaData) -> Table:
        return Table(IndexableLink.__collection_name__, meta_data,
              Column("link", String, primary_key= True, index=True),
              Column("scraped_on", DateTime, default=datetime.now())
         )
     
    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)
    

    def to_dict(self,) -> Dict[str, Any]:
        return {
            "link": self.link,
            "scraped_on": self.scraped_on
        }
    
