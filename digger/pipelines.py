# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

"""
This pipeline will check each link of DataLinkContainer's existance in database,
and allows only to pass unique one's.

Class has is_link_exist method which is wrapper around mongo's filter method to check 
existance inside database.
"""
class CollectionItemDuplicateFilterPiepline:
    pass


"""
Class is responsible for sanitizing, watermarks, html tags and confusing unicodes
all three are divided into three different methods, which will take vale of data.
This class will use default cleaning format provided in the collection_data,
provided during parse_node, they can contain, __watermarks as key.

Now waternmark remover only removes from begining or ending of the whole text corpus
"""

class CollectionItemSanitizingPipeline:
    pass

"""
The class will nicely pack the collection_data dict into DataLinkContainer and insert it into database
"""

class CollectionItemVaultPipeline:
    pass