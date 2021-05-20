from datetime import datetime
from typing import Dict, List, Optional, Union
from models.main import SourceMap, Format, ContentType

from secrets import token_urlsafe
import pandas as pd
from urllib.parse import urlparse


class SourcemapInterface:

    def create_map(self, name: str, home_link: str, formatter: str, links: List[Dict] = [], assumed_tags: str = '',
                   compulsory_tags: List[str] = [], is_rss: bool = False, is_collection: bool = True,
                   watermarks: List[str] = [], source_id: str = None, datetime_format: str = "", is_third_party: bool = False
                   ):
        return SourceMap.adapter().create(source_name=name, source_home_link=home_link,
                                          source_id=token_urlsafe(
                                              16) + "_" + name.lower() if source_id is None else source_id,
                                          formatter=formatter, assumed_tags=assumed_tags.strip(), links=links,
                                          compulsory_tags=compulsory_tags, is_collection=is_collection,
                                          is_rss=is_rss, watermarks=watermarks,
                                          datetime_format=datetime_format, is_third_party=is_third_party)
    
    def tag_formatter(self, tag:str) -> str:
        return tag.replace("/",".").replace("and", "")
    
    def get_collection_selector(self,name, extras = ['itertag'], defaults= {}):
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
    def is_quit_param(txt: str) -> bool:
        return txt.lower() == "q" or txt == "" or txt == " " or txt.lower() == "quit"
    
    def _create_collection_format(self):
        data = {}
        is_xml = input('Enter x for xml_collection_format or h for html_collection_format: ').lower() == 'x'
        data['title'] = self.get_collection_selector('title', defaults={"itertag": "items"})
        data['link'] = self.get_collection_selector('link', defaults={"itertag": "items"})
        data['creator'] = self.get_collection_selector('creator', defaults={"itertag": "items"})
        while True:
            sec_act = input("Enter name for new selector or type 'q' quit for exiting: ")
            if self.is_quit_param(sec_act):
                break
            else:
                data[sec_act] = self.get_collection_selector(sec_act, defaults={"itertag": "items"})
        return data
    
    def create_collection_format(self, name, format_: Format):
        data = self._create_collection_format()
        Format.adapter().update_one({"format_id": format_.format_id}, **{name:data})
        setattr(format_, name, data)
        return format_
    

