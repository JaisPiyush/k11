# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from datetime import datetime
from os import link
from models.main import DataLinkContainer
from models.postgres import IndexableLinks
from scrapy.exceptions import DropItem





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
        return DataLinkContainer.adapter().create(item.to_dict())

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
        