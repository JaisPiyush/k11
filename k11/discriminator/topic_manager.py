import os
from typing import Union
from k11.models.discriminator import Topics
from posixpath import dirname
import pickle

class TopicsManager:

    topics: Topics = None
    file_name: str = None
    path: str = None


    def set_file_name(self, name: str):
        self.file_name = name
    
    def set_file_path(self, file_path: str):
        self.path = file_path

    def get_module_path(self, path: str = None):
        """
        get_module_path will calculate the absolute path of destination file,
        the only condition is, that the target path must be spawned from same 
        parent dir of the python-script file.

        main
          |-- main.py
          |
          |-- __init__.py

        main.py <--- File running `get_module_path`
        taget folder must be child of main directory
        """
        if path is None or path == self.file_name:
            if self.path is not None and self.file_name is not None:
                return os.path.join(self.path, self.file_name)
            elif self.path is None and self.file_name is not None:
                return self.file_name
        parent_dir = os.path.abspath(dirname(__file__))
        path = os.path.join(parent_dir, path)
        return path
    
    def is_path_exist(self, path):
        return os.path.exists(path)
    
    def _load_module(self, path):
        """
        load the module file containing pickled object
        {"data": value, "contains_topic_class_values": False}
        data value hold the dict | str | number | list 
        and contains_topic_class_values if flag indicating whether the data must be de-serialized into Topic objectt
        """
        if self.is_path_exist(path):
            with open(path, "rb") as file:
                loaded_data = pickle.load(file)
                if "contains_topic_class_values" in loaded_data and loaded_data["contains_topic_class_values"]:
                    return Topics.from_dict(loaded_data["data"])
                return loaded_data
        return None
    
    def load_module(self, name: str = None):
        if name is None:
            name = self.file_name
        self.topics = self._load_module(self.get_module_path(path=self.file_name))
    
    def get_topics(self):
        if self.topics is None:
            return self.load_module()
        return self.topics
    


    
    def _dump_module(self, path, obj, contains_topic_class_values=False):
        if isinstance(obj, Topics):
            obj = obj.to_dict()
        picklable_object = {"data": obj,"contains_topic_class_values": contains_topic_class_values}
        with open(path,  "wb") as file:
            pickle.dump(picklable_object, file=file)
    
    def _insert_topic(self, topic: Topics, name: str):
        topic.append(name)
    
    def _remove_topic(self, topic: Topics, name: str):
        topic.remove(name)
    
    def insert_topic(self, name: str):
        self._insert_topic(self.get_topics(), name)
    
    def remove_topic(self, name: str):
        self._remove_topic(self.get_topics(), name)
    
    def _set_target_int(self, topics: Topics, name: str, target: int):
        topics.set_target_int(name, target)
    
    def set_target_int(self, name: str, target: int):
        self._set_target_int(self.get_topics(), name, target)
    
    def _get_target_name(self, topics: Topics, target: int):
        return topics.get_target_name(target)
    
    def _get_target_int(self, topics: Topics, name: str):
        return topics.get_target_int(name)
    
    def get_target_int(self, name: str):
        return self._get_target_int(self.get_topics(), name)
    
    def get_target_name(self, target: int):
        return self._get_target_name(self.get_topics(), target)
    
    def clean(self):
        self.file_name = None
        self.path = None
        self.topics = None

    
    def commit(self):
        # Cautions: Use only for topics object
        if self.file_name is None:
            raise ValueError("File Name is not provided.")
        if self.topics is not None:
            path = self.get_module_path()
            self._dump_module(path, self.topics, contains_topic_class_values=True)


class FrozenTopicInterface(TopicsManager):

    def __init__(self) -> None:
        super().__init__()
        self.set_file_name("topics_repository.bin")

    


    
