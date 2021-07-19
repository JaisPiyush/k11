from vault.adapter import TableAdapter
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TableModels(Base):

    adapter = TableAdapter()

    def __new__(cls, *args, **kwargs):
        cls.adapter.add_contribution(cls)
        return super(TableModels, cls).__new__(cls)
        
