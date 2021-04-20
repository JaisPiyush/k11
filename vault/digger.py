from vault.adapter import TableAdapter, register_digger
from sqlalchemy import select


class IndexableLinkAdapter(TableAdapter):
    """
    The method will use select statement to check whether the given link already exists
    """
    def is_link_travelled(self, link: str) -> bool:
        digger = register_digger()
        statement = select([self.table]).where(self.table.c.link == link)
        return digger.postgres_connection.execute(statement)
    
        
        
