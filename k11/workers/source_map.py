from datetime import datetime
from typing import Dict, List, Optional, Union
from models.main import SourceMap, Format, ContentType, LinkStore

from secrets import token_urlsafe
import pandas as pd
from urllib.parse import urlparse

class SourcemapInterface:
    
    @staticmethod
    def create_map(name: str, home_link: str, formatter: str,links: List[Dict] = [], assumed_tags: str = '',
                     compulsory_tags: List[str] = [], is_rss: bool = False, is_collection: bool = True,
                     watermarks: List[str] = [], source_id: str = None, datetime_format: str="", is_third_party: bool = False
                      ):
                      sm =  SourceMap(source_name=name, source_home_link=home_link,
                      source_id=token_urlsafe(16) + "_"+ name.lower() if source_id is None else source_id, 
                      formatter=formatter, assumed_tags=assumed_tags.strip(), links=links,
                      compulsory_tags=compulsory_tags, is_collection=is_collection,
                      is_rss=is_rss, watermarks=watermarks,
                      datetime_format=datetime_format, is_third_party=is_third_party
                      )
                      sm.save()
    
    @staticmethod
    def tag_formatter(tag:str) -> Union[str, None]:
        return tag.replace("/",".").replace("and", "")
    
    @staticmethod
    def get_collection_selector(name, extras = [], defaults= {}):
        _defaults = {"sel": "xpath", "param": "text()", "type": "text", "parent": name}
        _defaults.update(defaults)
        data = {}
        for key in ['sel', 'param', 'parent', 'type'] + extras:
            value = input(f"Please enter {key} value for {name}: ")
            if value == "" or value == " " and key in _defaults:
                data[key] = _defaults[key]
            else:
                data[key] = value
        return data
    
    @staticmethod
    def is_quit_param(txt):
        return txt.lower() == "q" or txt == "" or txt == " " or txt.lower() == "quit"
    
    def _create_collection_format(self):
        data = {}
        # is_xml = input('Enter x for xml_collection_format or h for html_collection_format: ').lower() == 'x'
        itertag = input('Enter itertag for this selector: ')
        if itertag != '' or itertag != " ":
            data['itertag'] = itertag
        data['title'] = self.get_collection_selector('title')
        data['link'] = self.get_collection_selector('link')
        data['creator'] = self.get_collection_selector('creator')
        while True:
            sec_act = input("Enter name for new selector or type 'q' quit for exiting: ")
            if self.is_quit_param(sec_act):
                break
            else:
                data[sec_act] = self.get_collection_selector(sec_act)
        return data
    
    def create_collection_format(self, name, format_: Format):
        data = self._create_collection_format()
        Format.adapter().update_one({"format_id": format_.format_id}, **{name:data})
        setattr(format_, name, data)
        return format_
    
    def get_container_identity(self):
        data = {}
        data['param'] = input("Enter param for container identity: ")
        data['is_mulitple'] = input("Is this identity for multiple items y/n: ").lower() == "y"
        data['content_type'] = input('Enter content-type for the identity a for article, i for image, v for video: ')
        cmap = {'a': 'article', 'i': 'image', 'v': 'video'}
        data['content_type'] = cmap[data['content_type']] if data['content_type'] != "" else cmap['a']
        data['is_bakeable'] = input('Is this identity contains multiple articles y/n: ').lower() == "y"
        title_selectors = []
        while True:
            action = input('Enter title selector or "q" for quit: ')
            if self.is_quit_param(action):
                break
            else:
                title_selectors.append(action)
        creator_selectors = []
        while True:
            action = input('Enter creator selector or "q" for quit: ')
            if self.is_quit_param(action):
                break
            else:
                creator_selectors.append(action)
        body_selectors = []
        while True:
            action = input('Enter body selector or "q" for quit: ')
            if self.is_quit_param(action):
                break
            else:
                body_selectors.append(action)
        return data, title_selectors, creator_selectors, body_selectors
    
    