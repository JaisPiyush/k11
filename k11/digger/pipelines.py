# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from dataclasses import replace
from k11.digger.abstracts import BaseSpider
from hashlib import sha256
from k11.models.no_sql_models import DataLinkContainer, Format, ArticleContainer, SourceMap
from k11.models.sql_models import IndexableArticle, IndexableLinks
from typing import Dict, List, Union
from urllib.parse import urlparse

from scrapy.spiders import Spider
from k11.models.main import ContainerFormat, ContainerIdentity, ContentType, QuerySelector
from scrapy.exceptions import DropItem
from bs4 import BeautifulSoup
import re
from scrapy.selector import Selector
from w3lib.html import remove_comments, remove_tags_with_content, replace_escape_chars
from k11.vault import connection_handler
import re

"""
This pipeline will check each link of Item Dict existance in database,
and allows only to pass unique one's.

Class has is_link_exist method which is wrapper around mongo's filter method to check 
existence inside database.
"""

def connect_sql_session():
    connection_handler.mount_sql_engines()
class CollectionItemDuplicateFilterPipeline:
     
    """
    This method will check the exist in all travelled links so far.
    Postgres will be used for this work
    """
    def link_already_exist_in_db(self, link: str) -> bool:
        return IndexableLinks.select().where(IndexableLinks.link == link).exists()

    
    """
    Process item will check if item['data']['link'] key exists and link_is_present_in_db?
    If exists the rain DropItem
    else process
    """
    def process_item(self, item: DataLinkContainer, spider):
        item = DataLinkContainer(**item)
        # print(item)
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

    def sanitize_text(self, text, spider: Spider = None, watermarks=None):
        soup = BeautifulSoup(text, 'html.parser')
        cleansed_text = soup.getText()
        return cleansed_text
    
    def process_item(self, item: DataLinkContainer, spider: Spider):
        if item.link != None and len(item.link) > 0:
            for key, value in item.container.items():
                if value is not None:
                    if isinstance(value, list):
                        item.container[key] = [self.sanitize_text(val, watermarks=item.watermarks, spider=spider) for val in value]
                    else:
                        item.container[key] = self.sanitize_text(value, watermarks=item.watermarks, spider=spider)
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
    def insert_container_in_db(self, item: DataLinkContainer) -> DataLinkContainer:
        if isinstance(item, DataLinkContainer):
            item.save()
        else:
            item = DataLinkContainer(**item)
            item.save()
        return item

    def index_link_into_db(self, item: DataLinkContainer):
        IndexableLinks.create(link=item.link, scraped_on=item.scraped_on,
                    source_name=item.source_name
        )

    def process_item(self, item: DataLinkContainer, spider):
        if item.link is not None and item.source_name is not None:
            item = self.insert_container_in_db(item)
            self.index_link_into_db(item)
            return item
        raise DropItem("Previous pipeline is sending lose data")


"""
This program handles the extraction, cleansing and packaging part
The program workflows like
1. process_item (item: {"iden", "data", "container", "format", "url"})
    if every data is not None:
        if data["html"] is str: Single Big article
            process_article
            if iden["is_bakable"]:
                process_baking
        elif data['html'] is list:
            process_multiple_articles
2. process_article(body, disabled , container, format_, url) | pr


"""

"""
item: {
    article: str
    format: ContainerFormat
    container: DataLinkContainer
    url: str
}
"""
class ArticleSanitizer:

    container: DataLinkContainer = None
    format_: ContainerFormat = None
    iden: ContainerIdentity = None
    original_iden: ContainerIdentity = None
    url: str = None
    content: str = None
    disabled: List[str] = []

    def process_attrs(self, item: Dict):
        self.container = item["container"]
        self.format_  = item['format']
        self.iden = ContainerIdentity(**item['iden']) if 'iden' in item and  item['iden'] != 'body' else ContainerIdentity('body', is_multiple=False, content_type=ContentType.Article)
        self.original_iden = self.iden
        self.content = item['content']
        self.disabled = item['disabled']
        self.url = item['url']

    @staticmethod
    def select_property(key:str, selector: Selector, container: DataLinkContainer = None,format_: ContainerFormat = None, format_key: str = None, default: str = None) -> str:
        value = None
        if format_ is not None and format_key is not None and hasattr(format_, format_key) and (properties := getattr(format_, format_key)) != None:
            for prop in properties:
                value = selector.xpath(prop).get()
                if value is not None:
                    break
        if value is None and container is not None and container.container is not None and key in container.container:
            value = container.container[key]
        else:
            return default
        return value
    
    """
    Different methods to extract title, if format has title_selectors, then extract title based on that if any matches,
    else go for containers default title
    """
    def get_title(self, selector: Selector) -> str:
        return self.select_property("title", selector, container=self.container, format_=self.format_, 
                                    format_key="title_selectors", default="")
    
    """
    Does similary thing as done above
    """
    def get_creator(self, selector: Selector) -> str:
        return self.select_property("creator", selector, container=self.container, format_=self.format_,
                                    format_key="creator_selectors", default="")
        
    """
    This function is specially made for bakeable articles, which are broken articles from onee big article,
    it basically scrap out text from that piece
    """
    def get_body(self, selector: Selector) -> Union[str, None]:
        if self.format_ is not None and self.format_.body_selectors is not None and len(self.format_.body_selectors) > 0:
            for body_selector in self.format_.body_selectors:
                # must return string not html
                value = selector.xpath(body_selector).get()
                if value is not None:
                    return value
        return None


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
    
    def flush_unrequited(self, text_set: List[str]) -> List[str]:
        # return [self.flush_emojis_and_hastag(self.flush_urls(text)) for text in text_set]
        return [self.flush_urls(txt) for txt in text_set]
     
    """
    Removes comment, script, noscript, style and b tags and striping all'\n\,\t,\r
    
    """
    def simple_cleansing(self, body:str) -> str:
        body = remove_comments(body)
        body = remove_tags_with_content(body, which_ones=('script', 'noscript', 'style'))
        body = replace_escape_chars(body, which_ones=('\n', '\t','\r'))
        return body
    
    """
    This is differed cleaning that is the html tags will be removed completely
    """
    def deffer_cleaning(self, body: str) -> str:
        # body = replace_tags(body)
        return body

    
    """
    Extract contents like images, videos, text_set, title, creator, and body
    """
    def extract_contents(self, body_unselected: str) -> Dict:
        body = Selector(text=body_unselected)
        data = {"images": [], "videos": [], "text_set": None, "disabled": self.disabled, "body": None}
        data["images"] = body.xpath('///img/@src').getall()
        data["images"] += body.xpath('///picture//source/@src').getall()
        data["videos"] = body.xpath('///video/@src').getall()
        data['text_set'] = [body_unselected]
        data['title'] = self.get_title(body)
        data["creator"] = self.get_creator(body)
        if self.iden['is_multiple'] and self.original_iden.is_bakeable and (sub_body := self.get_body(body)) != None:
            data['body'] = self.deffer_cleaning(self.simple_cleansing(sub_body))
        return data
    
    @staticmethod
    def is_source_present_in_db(home_link: str) -> bool:
        return Format.objects(source_home_link = home_link).count()  > 0


    """
    This function has all the responsibility to pack all the messed data into neat and clean article container
    """
    def pack_container(self, url: str,title:str = "", creator: str = "",images: List[str] = [],
                        disabled: List[str]= [], videos: List[str]= [],
                        text_set: List[str] = [], body: str= None, index: int = 0, tags: List[str] = []) -> ArticleContainer:
        parsed = urlparse(url)
        return ArticleContainer(
            article_id=sha256(url.encode()).hexdigest() + str(index),
            title=title,
            source_name=self.container.source_name,
            source_id=self.container.source_id,
            article_link=url,
            creator=creator,
            scraped_from=self.container.link,
            home_link=f"{parsed.scheme}://{parsed.netloc}",
            site_name=self.container.container['site_name'] if 'site_name' in self.container.container else self.container.source_name,
            scraped_on=self.container.scraped_on,
            pub_date=self.container.container['pub_date'] if 'pub_date' in self.container.container else None,
            disabled=disabled,
            is_source_present_in_db=self.is_source_present_in_db(f"{parsed.scheme}://{parsed.netloc}"),
            tags=tags,
            compulsory_tags=self.container.compulsory_tags if self.container.compulsory_tags is not None else [],
            images=images,
            videos=videos,
            text_set=text_set,
            body=body,
            majority_content_type=self.iden['content_type'] if hasattr(self.iden, 'content_type') else ContentType.Article,
            next_frame_required=self.iden['content_type'] == ContentType.Article
        )

    
    def process_article(self, body:str, url: str, index=0) -> ArticleContainer:
        data = self.extract_contents(body_unselected=body)
        data["index"] = index
        data["tags"] = self.container.assumed_tags.split(" ") if self.container is not None and self.container.assumed_tags is not None else ""
        return self.pack_container(url=url, **data)
    
    """
    This method search for another iden in format which has is_mulitple = True and is available ont the current site
    """
    def process_baking(self,url: str, body: str) -> List[ArticleContainer]:
        _selector = Selector(text=body)
        for iden in self.format_.idens:
            if iden != self.original_iden and iden.is_multiple:
                self.iden = iden
                return [self.process_article(content=semi_articles, url=url, index=index + 1) for index, semi_articles in enumerate(_selector.css(iden['param']).getall())]
        return []  


    
    def process_multiple_article(self, url: str, body: List[str],) -> List[ArticleContainer]:
        return [self.process_article(content, url, index=index) for index, content in enumerate(body)]

    def process_item(self, item: Dict, spider) -> List[ArticleContainer]:
        self.process_attrs(item)
        articles: List[ArticleContainer] = []
        if isinstance(self.content, str):
            # Single article with possibility of being bakeable
            # First task is to create the giant article
            articles.append(self.process_article(body=self.content, url=self.url))
            if self.iden['is_bakeable']:
                articles += self.process_baking(ulr=self.url, body=self.content)
        else:
            articles = self.process_multiple_article(url=self.url, body=self.content)
        if len(articles) == 0:
            DropItem("No item came out eventually.")
        return articles
        

"""
Remove duplicate items and define major content type and also remove urls and emojis, '#' from text.
Article :- Includes Text, Video, Image
Images :- One or more Images, page transition will not happen if len(text) < 51 or content == None
Videos :- One or more Images, page transition will not happen if source is youtube or len(text) < 51 or content == None
"""

class ArticleDuplicateFilter:
    
    # Return True if article is present inside the database
    def is_article_present_in_db(self, article_id: str) -> bool:
        return IndexableArticle.select().where(IndexableArticle.article_id == article_id).exists()
    
    def process_item(self, items: List[ArticleContainer], spider: BaseSpider):
        if len(items) == 0:
            raise DropItem("No One came in")
        return [item for item in items if not self.is_article_present_in_db(item.article_id)]
        


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
    def process_item(self, items: List[ArticleContainer], spider):
        if len(items) > 0:
            ArticleContainer.objects.insert(items)
            indexable_articles = map(lambda x: IndexableArticle.from_article_container(x), items)
            IndexableArticle.insert_many(indexable_articles).execute()
            DataLinkContainer.objects.delete_containers([item.scraped_from for item in items if item.scraped_from is not None])
            return items
        raise DropItem("Dropped in ArticleVaultPipeline")



class ArticlePreprocessor:
    """
    Remove all ignoreables from the text, along with comments, tags, urls and escape characters
    Extract All images, videos and text_set.
    """

    def get_all_ignoreable_elements(self, soup: BeautifulSoup, ignoreable: QuerySelector ):
        tag = ""
        all_ignoreable_elements = []
        if ignoreable.tag is not None and len(ignoreable.tag) > 0:
            tag = ignoreable.tag
        if ignoreable.id is not None and len(ignoreable.id) > 0:
            all_ignoreable_elements.extend(list(soup.select(f"{tag}#{ignoreable.id}")))
        if ignoreable.class_list is not None and len(ignoreable.class_list) > 0:
            for cls_ in ignoreable.class_list:
                # print("."+cls_,soup.select(f".{cls_}"))
                all_ignoreable_elements.extend(list(soup.select(f".{cls_}")))
        if ignoreable.exact_class is not None and len(ignoreable.exact_class) > 0:
            all_ignoreable_elements.extend(list(soup.select(f"{tag}.{ignoreable.exact_class}")))
        if ignoreable.id is None and (ignoreable.class_list is None or len(ignoreable.class_list) == 0) and ignoreable.exact_class is None and ignoreable.tag  is not None:
            all_ignoreable_elements.extend(list(soup.select(tag)))
        return set(all_ignoreable_elements)

    def remove_ignoreables(self, text, formatter: ContainerFormat, **kwargs):
        soup = BeautifulSoup(text, 'html.parser')
        for ignoreable in formatter.get_ignoreables():
            for element in self.get_all_ignoreable_elements(soup, ignoreable):
                element.decompose()
        return soup

    """
    Cleaning all escape characters like \n,\t,\r
    (\t)+,(\n)+,(\r)+,(\t\n)+,(\n\t)+,(\r\t)+,(\t\r)+,(\r\n)+,(\r\n)+
    """
    def clean_residuals(self, text:str) -> str:
        text = re.sub(r'(\t)+', '\t', text)
        text = re.sub(r'(\n)+', '\n', text)
        text = re.sub(r'(\r)+', '\r', text)
        text = re.sub(r'(\t\r)+', '\t\r', text)
        text = re.sub(r'(\r\t)+', '\r\t', text)
        text = re.sub(r'(\n\t)+', '\n\t', text)
        text = re.sub(r'(\t\n)+', '\t\n', text)
        text = re.sub(r'(\r\n)+', '\r\n', text)
        text = re.sub(r'(\n\r)+', '\n\r', text)
        return text
    
    def extract_items(self, soup: BeautifulSoup) -> Dict[str, List]:
        data = {"images": [], "videos": []}
        data["images"] = [img["src"] for img in soup.find_all("img")]
        data["images"] += [source['src'] for source in soup.select('picture > source')]
        data["videos"] = [video["src"] for video in soup.find_all("video")]
        #youtube embedded links
        data["videos"] += [video["src"] for video in soup.select("iframe.youtube-player")]
        return data
    
    def process_item(self, item: dict, spider: BaseSpider):
        if item["content"] is not None and item["formatter"] is not None:
            soup = self.remove_ignoreables(item['content'], item['formatter'])
            media = self.extract_items(soup)
            item.update(media)
            item["body"] = self.clean_residuals(soup.get_text())
            return [self.pack_container(**item)]
        else:
            raise DropItem("Invalid item packing")
    
    def is_source_present_in_db(self, url: str) -> bool:
        return SourceMap.objects(source_home_link=url).count() > 0
    
    def pack_container(self, url: str,images: List[str] = [],videos: List[str]= [],iden: ContainerIdentity = None,
                        link_container: DataLinkContainer = None,
                        body: str= None, index: int = 0, **kwargs) -> ArticleContainer:
        parsed = urlparse(url)
        return ArticleContainer(
            article_id=sha256(url.encode()).hexdigest() + str(index),
            title=link_container.container['title'],
            source_name=link_container.source_name,
            source_id=link_container.source_id,
            article_link=url,
            creator=link_container.container['creator'],
            scraped_from=link_container.link,
            home_link=f"{parsed.scheme}://{parsed.netloc}",
            site_name=link_container.container['site_name'] if 'site_name' in link_container.container else link_container.source_name,
            scraped_on=link_container.scraped_on,
            pub_date=link_container.container['pub_date'] if 'pub_date' in link_container.container else None,
            disabled=[],
            is_source_present_in_db=self.is_source_present_in_db(f"{parsed.scheme}://{parsed.netloc}"),
            tags=link_container.assumed_tags.split(" "),
            compulsory_tags=link_container.compulsory_tags if link_container.compulsory_tags is not None else [],
            images=images,
            videos=videos,
            body=body,
            majority_content_type=iden['content_type'] if hasattr(iden, 'content_type') else ContentType.Article,
            next_frame_required=iden['content_type'] == ContentType.Article
        )

        

