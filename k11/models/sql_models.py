from datetime import datetime
from mongoengine.fields import BooleanField
from peewee import AutoField, CompositeKey, ForeignKeyField, Model, CharField,DateTimeField
from k11.vault.app import connection_handler
from playhouse.postgres_ext import ArrayField, TSVectorField

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


class BaseTreasureModel(Model):

    class Meta:
        database = connection_handler.engines["postgres_digger"]



class ContentTag(BaseTreasureModel):
    tag_id = AutoField(primary_key=True)
    tag_name = CharField(max_length=75)
    parent = ForeignKeyField('self', backref='children')
    complete_name = CharField(max_length=300, db_index=True)



class Article(BaseTreasureModel):
    article_id = CharField(max_length=300, index=True, primary_key=True)
    title = CharField(max_length=300, index=True)
    title_vector = TSVectorField()
    scraped_on = DateTimeField(default=datetime.now())
    images = ArrayField(CharField)
    videos = ArrayField(CharField)
    source_name = CharField(max_length=100, index=True)
    source_home_link = CharField(max_length=2000)
    url = CharField(max_length=2000, index=True)
    dates_str = ArrayField(CharField)
    dates = ArrayField(DateTimeField)
    is_body_exist = BooleanField(default=True)
    content_type = CharField(max_length=20)
    navigation_to_url_required = BooleanField(default=True)


class ContentTagArticle(BaseTreasureModel):
    tag = ForeignKeyField(ContentTag, backref="articles")
    article = ForeignKeyField(Article, backref="tags")
    created_on = DateTimeField(default=datetime.now())

    class Meta:
        primary_key = CompositeKey('tag', 'article')


class KeywordEntity(BaseTreasureModel):
    id = AutoField(primary_key=True)
    name = CharField(max_length=300, index=True)
    name_vector = TSVectorField()
    ent_type = CharField(max_length=15)
    created_on = DateTimeField(default=datetime.now())

class KeywordsArticle(BaseTreasureModel):
    keyword = ForeignKeyField(KeywordEntity, backref="articles")
    article = ForeignKeyField(Article, backref="keywords")

    class Meta:
        primary_key = CompositeKey("keyword", "article")





