from datetime import datetime
from peewee import Model, CharField,DateTimeField
from k11.vault.app import connection_handler

connection_handler.mount_sql_engines()

class IndexableLinks(Model):
    link = CharField(max_length=2000, primary_key=True, index=True)
    scraped_on = DateTimeField(default=datetime.now())
    source_name = CharField(max_length=75)

    class Meta:
        database = connection_handler.engines['postgres_digger']



class IndexableArticle(Model):
    article_id = CharField(max_length=300, primary_key=True)
    link = CharField(max_length=2000, index=True)
    site_name = CharField(max_length=70)
    scraped_on = DateTimeField(default=datetime.now())
    pub_date = DateTimeField(default=datetime.now())

    class Meta:
        database = connection_handler.engines['postgres_digger']

    @staticmethod
    def from_article_container(article):
        return {
            "article_id":article.article_id,
            "link":article.article_link,
            "site_name":article.site_name,
            "scraped_on":article.scraped_on,
            "pub_date":article.pub_date
        }


# connection_handler.mount_sql_engines()