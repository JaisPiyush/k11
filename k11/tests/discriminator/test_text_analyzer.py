from unittest import TestCase
from k11.discriminator.text_analyzer import CorpusManager, DictionaryManager, TextAnalyzer, TextProcessor


class TestTextProessor(TestCase):
    text_processor = TextProcessor(package="en_core_web_trf")

    def test_stop_word_identification(self):
        words = [("piyush", False), ("computing", False), ("of", True), ("name", True), ("is", True), 
                    ("was", True), ("arguing", False), ("?", True), ("", True)]
        for word, out in words:
            self.assertEqual(self.text_processor._is_stopword(word), out)
    
    def test_extract_ner(self):
        docs = [{
            "text": "Apple Inc. is a big company",
            "org": ["Apple Inc."],
            "gpe": [],
            "per": [],
        }, {
            "text": "Piyush Jaiswal is good man.",
            "per": ["Piyush Jaiswal"],
            "org":[],
            "gpe": []
        }, {
            "text": "Apple Inc. is the company based in Bengaluru, where Sumant Singh works",
            "per": ["Sumant Singh"],
            "org": ["Apple Inc."],
            "gpe": ["Bengaluru"]
        }]
        for doc in docs:
           extracts = self.text_processor._extract_ner(doc["text"])
           self.assertEqual(extracts["per"], doc["per"])
           self.assertEqual(extracts["org"], doc["org"])
           self.assertEqual(extracts["gpe"], doc["gpe"])
 


class TestCorpusManager(TestCase):

    corpus_manager = CorpusManager()
    corpus = []

    def test_load_and_save(self):
        filename = "test_corpus.mm"
        file_path = self.corpus_manager.get_new_file_path(filename)
        test_corpus = [[(1, 0.3), (2, 0.1)], [(1, 0.1)], [(2, 0.3)]]
        self.corpus_manager._save_corpus(file_path, test_corpus)
        test_corpus_2 = [[(4, 0.3), (8, 0.1)], [(12, 0.1)], [(6, 0.3)]]
        self.corpus_manager._save_corpus(file_path,test_corpus_2)
        self.corpus = test_corpus + test_corpus_2
        self.assertListEqual(self.corpus, [corp for corp in self.corpus_manager._load_corpus(file_path)] )

    
   

class TestDictionaryManager(TestCase):

    dictionary_manager = DictionaryManager()

    def test_save_dictionary(self):
        texts = [["hello", "world", "we", "are", "here"]]
        self.dictionary_manager.digest(texts)
        self.dictionary_manager.save()
        tokens = self.dictionary_manager.get_token_to_id_map().keys()
        diff = set(tokens).difference(set(texts[0]))
        self.assertEqual(diff == set(), True)




