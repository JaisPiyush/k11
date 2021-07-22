from datetime import datetime
from k11.vault.connections import SqlBase
from sqlalchemy import Column, DateTime, String



class IndexableLinks(SqlBase):
    __tablename__ = "indexable_links"
    __databasename__ = "postgres_digger"
    link = Column(String(2000), primary_key=True, index=True)
    scraped_on = Column(DateTime, default=datetime.now())
    source_name = Column(String(75))

class IndexableArticle(SqlBase):
    __tablename__ = "article_indexable_link"
    __database__ = "postgres_digger"
    article_id = Column(String(260),index=True, primary_key=True)
    link = Column(String(2000),index=True)
    site_name = Column(String(70))
    scraped_on = Column(DateTime, default=datetime.now())
    pub_date = Column(DateTime, default=datetime.now())

    @classmethod
    def from_article_container(cls, article):
        return cls(
            article_id=article.article_id,
            link=article.article_link,
            site_name=article.site_name,
            scraped_on=article.scraped_on,
            pub_date=article.pub_date
        )


# connection_handler.mount_sql_engines()