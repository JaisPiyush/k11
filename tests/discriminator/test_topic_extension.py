
from posixpath import dirname
from unittest import TestCase
from k11.models.discriminator import Topics
from k11.discriminator.topic_manager import TopicsManager
import os


def module_paths():
    return [{
        "path": "filers/file_exist_test.txt",
        "out": True
    }, {
        "path": "filers/file_not_exits_test.bin",
        "out": False
    },]

def loadable_modules():
    return [{
        "path": "filers/file_exist_test.bin",
        "out": False
    }]

class TestTopicExtension(TestCase):
    topic_manager = TopicsManager()
    
    def test_get_module_path(self):
        for module_path_data in module_paths():
            result = self.topic_manager.get_module_path(path=module_path_data["path"])
            result = self.topic_manager.is_path_exist(result)
            self.assertEqual(result, module_path_data["out"], f"{module_path_data['path']} is failing the test")
        dir_name = dirname(__file__)
        dir_name = "/".join((dir_name.split("/")[:-1]))
        path = os.path.abspath(os.path.join(dir_name, "filers"))
        file_name = "file_exist_test.txt"
        self.topic_manager.set_file_name(file_name)
        self.topic_manager.set_file_path(path)
        result = self.topic_manager.get_module_path()
        self.assertEqual(result, os.path.join(path, file_name))
        

    def test_load_module(self):
        for module in loadable_modules():
            path = os.path.abspath(os.path.join(dirname(__file__), module["path"]))
            result = self.topic_manager._load_module(path)
            self.assertEqual(result is not None, module["out"])


    def test_dump_module(self):
        obj = {"name": "Piyush"}
        dir_name = dirname(__file__)
        dir_name = "/".join((dir_name.split("/")[:-1]))
        path = os.path.abspath(os.path.join(dir_name, "filers/file_exist.bin"))
        self.topic_manager._dump_module(path, obj, contains_topic_class_values=False)
        result = self.topic_manager._load_module(path)
        self.assertEqual(result is not None, True )
        self.assertEqual(result["data"], obj)

    def test_topic_objecs(self):
        topic = Topics()
        # Testing append 
        topic.append("test_topic_1")
        self.assertListEqual(topic.names, ["test_topic_1"])

        # Testing __add__
        topic = topic + ["test_topic_2"]
        self.assertListEqual(topic.names, list(set(["test_topic_1", "test_topic_2"])))

        # Testing remove
        topic.remove("test_topic_2")
        self.assertListEqual(topic.names, ["test_topic_1"])

        # Testing contains
        self.assertEqual(topic.contains("test_topic_1"), True)

        # Testing lengths
        self.assertEqual(len(topic), 1)
        self.assertEqual(topic.target_len(), 0)

        # Testing set_target
        topic.set_target_int("test_topic_1", 1)

        # Testing get_target_int
        self.assertEqual(topic.get_target_int("test_topic_1"), 1)
        self.assertEqual(topic.get_target_name(1), "test_topic_1")

        topic.set_target_int("test_topic_2", 2)
        self.assertEqual(topic.get_target_name(2), "test_topic_2")
        self.assertEqual(len(topic),2 )
        topic.remove("test_topic_1")
        
        # Final Testing
        self.assertEqual(len(topic), 1)
        self.assertEqual(topic.target_len(), 1)


    def test_insert_topic(self):
        topics = Topics(names=["science", "politics", "sports"])
        self.topic_manager._insert_topic(topics, "finance")
        self.assertEqual(topics.contains("finance"), True)        


    def test_remove_topic(self):
        topics = Topics(names=["science", "politics", "sports"])
        self.topic_manager._remove_topic(topics, "politics")
        self.assertEqual(topics.contains("politics"), False) 

    def test_set_target_int(self):
        topics = Topics(names=["science", "politics", "sports"])
        self.topic_manager._set_target_int(topics,"science", 0)
        self.assertEqual(self.topic_manager._get_target_name(topics, 0), "science")
    
    def test_clean(self):
        self.topic_manager.clean()
        self.assertEqual(self.topic_manager.topics, None)
        self.assertEqual(self.topic_manager.file_name, None)
        self.assertEqual(self.topic_manager.path, None)

        
    def test_commit(self):
        topics = Topics(names=["science", "politics", "sports"])
        file_name = "file_exist_2.bin"
        self.topic_manager.set_file_name(file_name)
        self.assertEqual(self.topic_manager.file_name, file_name)

        dir_name = dirname(__file__)
        dir_name = "/".join((dir_name.split("/")[:-1]))
        dir_name = os.path.join(dir_name, "filers")
        self.topic_manager.set_file_path(dir_name)
        self.assertEqual(self.topic_manager.path, dir_name)
        self.topic_manager.topics = topics

        self.assertNotEqual(self.topic_manager.topics, None)

        self.topic_manager.commit()
        self.topic_manager.clean()

        self.assertEqual(self.topic_manager.topics, None)

        self.topic_manager.set_file_name(file_name)
        self.topic_manager.set_file_path(dir_name)
        self.topic_manager.load_module()
        self.assertNotEqual(self.topic_manager.topics, None)
        self.assertListEqual(self.topic_manager.get_topics().names, topics.names)




