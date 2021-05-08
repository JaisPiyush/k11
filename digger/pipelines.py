# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from typing import List
from models.main import ArticleContainer, ContentType, DataLinkContainer
from models.postgres import IndexableArticle, IndexableLinks
from scrapy.exceptions import DropItem
from bs4 import BeautifulSoup
import re



from bs4 import BeautifulSoup


"""
This pipeline will check each link of Item Dict existance in database,
and allows only to pass unique one's.

Class has is_link_exist method which is wrapper around mongo's filter method to check 
existance inside database.
"""
class CollectionItemDuplicateFilterPiepline:
     
    """
    This method will check the existance in all travelled links so far.
    Postgres will be used for this work
    """
    def link_already_exist_in_db(self, link: str) -> bool:
        return IndexableLinks.adapter().exists(IndexableLinks.link == link)
    
    """
    Process item will check if item['data']['link'] key exists and link_is_present_in_db?
    If exists the rain DropItem
    else process
    """
    def process_item(self, item: DataLinkContainer, spider):
        if item.link != None and len(item.link) > 0 and not self.link_already_exist_in_db(item.link):
            return item
        raise DropItem("Item already exists")
        

            




"""
Class is responsible for sanitizing, watermarks, html tags and confusing unicodes
all three are divided into three different methods, which will take vale of data.
This class will use default cleaning format provided in the collection_data,
provided during parse_node, they can contain, __watermarks as key.

Now waternmark remover only removes from begining or ending of the whole text corpus
"""


class CollectionItemSanitizingPipeline:
    """

    """
    def sanitize_text(self, text, watermarks=None):
        soup = BeautifulSoup(text, 'html.parser')
        cleansed_text = soup.getText()
        if watermarks is not None:
            for watermark in watermarks:
                if cleansed_text.index(watermark) == 0:
                    cleansed_text = cleansed_text[len(watermark):]
                if cleansed_text.index(watermark) == len(cleansed_text) -1:
                    cleansed_text = cleansed_text[:len(text) - len(watermark)]
        return cleansed_text
    
    def process_item(self, item: DataLinkContainer, spider):
        if item.link != None and len(item.link) > 0:
            for key, value in item.container.items():
                item.container[key] = self.sanitize_text(value, watermarks=item.watermarks)
            return item
        raise DropItem("Link container was fatal, it was incomplete OK!! :(")

        


"""
The class will nicely pack the collection_data dict into DataLinkContainer and insert it into database 
and will also index the link into postgres
"""

class CollectionItemVaultPipeline:

    """
    Insert Link container document in mongodb
    """
    def insert_cotainer_in_db(self, item: DataLinkContainer) -> DataLinkContainer:
        return DataLinkContainer.adapter().create(**item.to_dict())

    def index_link_into_db(self, item: DataLinkContainer):
        IndexableLinks.adapter().create(link=item.link, scraped_on=item.scraped_on,
                    source_name=item.source_name
        )
    
    def process_item(self, item: DataLinkContainer, spider):
        if item.link is not None and item.source_name is not None:
            item = self.insert_cotainer_in_db(item)
            self.index_link_into_db(item)
            return item
        raise DropItem("Previous pipeline is sending lose data")



"""
Remove duplicate items and define major content type and also remove urls and emojis, '#' from text.
Article :- Includes Text, Video, Image
Images :- One or more Images, page transition will not happen if len(text) < 51 or content == None
Videos :- One or more Images, page transition will not happen if source is youtube or len(text) < 51 or content == None
"""

class ArticleDuplicateAndContentTypeFilter:

    def is_article_present_in_db(self, article_id: str) -> bool:
        return ArticleContainer.adapter().find_one({"article_id": article_id}, silent=True) != None
    
    def estimate_major_content(self, content: str) -> str:
        soup = BeautifulSoup(content)
        body = soup.find('body')
        if self.is_content_image(body):
            return ContentType.Image
        elif  self.is_content_video(body):
            return ContentType.Video
        return ContentType.Article
    
    def is_content_image(self, content) -> bool:
        tags = ['img', 'picture']
        for child in content.children:
            return child.name.lower() in tags
    
    def is_content_video(self, content) -> bool:
        tags = ['video']
        for child in content.children:
            return child.name.lower() in tags
    
    def find_urls(self, text: str) -> List[str]:
        regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
        return [x[0] for x in re.findall(regex, text)]

    def flush_urls(self, text: str) -> str:
        urls = self.find_urls(text)
        if urls is None or len(urls) == 0:
            return text
        for url in urls:
            text = text.replace(url, "")
        return text
    
    def find_emojis(self, text: str) -> List[str]:
        regex = r'[\u263a-\U0001f645]'
        return re.findall(regex, text)
    
    def flush_emojis_and_hastag(self, text:str) -> str:
        matches = self.find_emojis(text)
        if matches is None or len(matches) == 0:
            return text
        for match in matches:
            text = text.replace(match, "")
        return text.replace('#',"")
         
    
    def process_item(self, item: ArticleContainer, spider):
        if not self.is_article_present_in_db(item):
            item.next_frame_required = True
            if item.content is not None and item.majority_content_type is None:
                item.majority_content_type = self.estimate_major_content(item.content)
                if item.majority_content_type == ContentType.Video or item.majority_content_type == ContentType.Image:
                    item.next_frame_required = False          
            if item.text is not None and len(item.text) > 0:
                item.text = self.flush_emojis_and_hastag(self.flush_urls(item.text))
            return item
        raise DropItem("Content already exists.")


"""
This pipeline will store the content in treasure(mongo) db and create and
index of each article in postgres for maintaining duplicacy 
"""
class ArticleVaultPipeline:
    
    """
    Data Inserted into postgres treasure are highly optimized
    id -> AutoIncremented[PKey]
    mongo_article_id -> same as mongo_db
    title -> String
    creator -> String
    site_name -> String
    pub_date -> DateTime
    text_vectors -> ts_vector[GIN]
    category_vectors -> array[GIN]
    sentiments -> Int
    disabled -> Array
    dates -> Array[String]
    names -> Array
    places -> Array
    organisations -> Array
    keywords -> Array[String][GIN]
    article_link: LinkString
    coords: Array[GeoCodes]
    meta: JSON
    images: Array[LinkString]
    videos: Array[LinkString]
    scraped_on: DateTime
    home_link: LinkString
    source_id: String
    source_name: String
    priority_keywords: Array[String][GIN] 
    #IF these keywords belongs to someone they should be shown compulsorily
    criticality: float [0: normal, 0.5: should be prioritise if per person view reaches 2, 1: should be shown to each active relevant user upto 5 times]
    next_frame_required: Boolean # does the screep tap required to navigate to next screen in App
    
    """

    def process_item(self, item: ArticleContainer, spider):
        ArticleContainer.adapter().create(**item.to_dict())
        print(item.article_id)

"""
The class basically filters third party articles, i.e articles not scraped by the spiders
"""
class ThirdPartyArticlesDuplicateFilter(ArticleDuplicateAndContentTypeFilter):
    def process_item(self, item: ArticleContainer, spider=None):
        return super().process_item(item, spider)