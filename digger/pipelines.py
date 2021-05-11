# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from dataclasses import replace
from hashlib import sha256
from typing import Dict, List, Union
from urllib.parse import urlparse
from models.main import ArticleContainer, ContainerFormat, ContainerIdentity, ContentType, DataLinkContainer, Format
from models.postgres import IndexableArticle, IndexableLinks
from scrapy.exceptions import DropItem
from bs4 import BeautifulSoup
import re
from scrapy.selector import Selector
from w3lib.html import remove_comments, remove_tags_with_content, replace_escape_chars, replace_tags


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
        self.iden = ContainerIdentity(**item['iden'])
        self.original_iden = ContainerIdentity(**item["iden"])
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
    def deferd_cleaning(self, body: str) -> str:
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
        cleaned_body = Selector(text=self.simple_cleansing(body_unselected))
        if len(texts := "".join(cleaned_body.xpath('///text()').getall())) > 0:
            print("\n*20",texts,)
            _steps = 245
            data["text_set"] = []
            if len(texts) < _steps:
                data["text_set"] = [self.deferd_cleaning(txt) for txt in texts]
            else:
                for chunk in range(0, len(texts), _steps):
                    if chunk + _steps < len(texts):
                        print(texts[chunk: chunk + _steps])
                        data["text_set"].append(self.deferd_cleaning(texts[chunk: chunk + _steps]))
                    else:
                        print(texts[chunk:])
                        data["text_set"].append(self.deferd_cleaning(texts[chunk:]))
            print(data['text_set'])
            # data["text_set"] = self.flush_unrequited(data["text_set"])
        data['title'] = self.get_title(body)
        data["creator"] = self.get_creator(body)
        if self.iden['is_multiple'] and self.original_iden.is_bakeable and (sub_body := self.get_body(body)) != None:
            data['body'] = self.deferd_cleaning(self.simple_cleansing(sub_body))
        print(data)
        return data
    
    @staticmethod
    def is_source_present_in_db(home_link: str) -> bool:
        return Format.adapter().find_one({"source_home_link": home_link}, silent=True) != None


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
            compulsory_tags=self.container.compulsory_tags.split(" ") if self.container.compulsory_tags is not None else [],
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

    def process_item(self, item: Dict, spider) -> Union[ArticleContainer, List[ArticleContainer]]:
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

    def is_article_present_in_db(self, article_id: str) -> bool:
        return ArticleContainer.adapter().find_one({"article_id": article_id}, silent=True) != None
    
    def process_item(self, items: List[ArticleContainer], spider):
        if len(items) == 0:
            raise DropItem("No One came in")
        return list(filter(lambda item : not self.is_article_present_in_db(item.article_id), items))
        


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
            ArticleContainer.adapter().bulk_insert(items)
            DataLinkContainer.delete_containers([item.scraped_from for item in items if item.scraped_from is not None])
        return items

