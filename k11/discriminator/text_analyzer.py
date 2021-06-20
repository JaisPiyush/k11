from posixpath import dirname
from typing import Dict, Generator, Iterable, List, Tuple, NoReturn
import spacy
from k11.models.main import ArticleContainer
from k11.models.discriminator import CorpusHolder, TextMeta
from string import punctuation
from datetime import date, datetime
from gensim.corpora.mmcorpus import MmCorpus
from gensim.corpora import Dictionary
import os
from .topic_manager import FrozenTopicInterface, TopicsManager
from gensim.models.ldamodel import LdaModel



class TextProcessor:
    nlp = None
    
    def __init__(self, package="en_core_web_trf") -> None:
        self.nlp = spacy.load(package)
    
    def _is_not_stopword(self, word) -> bool:
        return len(word)  > 0 and self.nlp.vocab[word].is_stop == False and word not in punctuation
    
    def _is_stopword(self, word) -> bool:
        return not self._is_not_stopword(word)
    
    def is_stopword(self, token) -> bool:
        return self._is_stopword(token.text)
    
    def _extract_ner(self, text:str) -> Dict:
        doc = self.nlp(text)
        data = {"per": [], "org": [], "gpe": [], "keywords": []}
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                data["per"].append(ent.text)
            elif ent.label_ == "ORG":
                data["org"].append(ent.text)
            elif ent.label_ == "GPE":
                data["gpe"].append(ent.text)
        data["keywords"] += data["per"] + data["org"] + data["gpe"]
        return data
    
    def lemmatize_text(self, token, array, allowed_pos = ["NOUN", "VERB", "ADJ", "ADV"]) -> NoReturn:
        if token.pos_ in allowed_pos:
            array.append(token.lemma_)
    

    def _process_article(self, article: ArticleContainer) -> Tuple[List[str], TextMeta]:
        text_meta = TextMeta(article.article_id, article)
        text = " ".join(article.text_set)
        doc = self.nlp(text)
        text_corpus = []
        for token in doc:
            if not self.is_stopword(token):
                self.lemmatize_text(token, text_corpus)
        text_meta.adapt_from_dict(self._extract_ner(text))

        return text, text_meta
        
            

class CorpusManager:
    dir_path = os.path.join(os.path.abspath(dirname(__file__)), "corpus")
    time_format = "%d_%m_%Y"
    corpus_class = MmCorpus

    def file_name_generator(self):
        time = datetime.now()
        return time.strftime(self.time_format) + ".mm"
    
    def get_new_file_path(self, name):
        return os.path.join(self.dir_path, name)
    
    def get_corpus_file_paths(self):
        file_paths = []
        for name in os.listdir(self.dir_path):
            full_name = os.path.join(self.dir_path, name)
            if os.path.isfile(full_name) and name != "__init__.py" and name.endswith(".mm"):
                file_paths.append(full_name)
        return file_paths
    
    def is_file_exist(self, fname) -> Tuple[bool, str]:
        full_path = os.path.join(self.dir_path, fname)
        return os.path.exists(full_path), full_path 
    
    def check_corpus_directory(self):
        return len(self.get_corpus_file_paths()) > 0


    def _load_corpus(self, path):
        return self.corpus_class(path)
    
    def _save_corpus(self, fname, corpus):
        is_exists, file_path = self.is_file_exist(fname)
        if is_exists:
            old_corpus = [corp for corp in self.corpus_class(file_path)]
            old_corpus += corpus
            corpus = old_corpus
        self.corpus_class.serialize(fname, corpus)

    def save_corpus(self, corpus):
        self._save_corpus(self.get_new_file_path(self.file_name_generator()), corpus)



class DictionaryManager:
    dir_path = os.path.abspath(dirname(__file__))
    path = os.path.join( dir_path, "dictionary_state_lda.dict")
    dictionary_class = Dictionary

    def __init__(self, forced_empty=False) -> None:
        if forced_empty or self.get_file_path() is None:
            self.dictionary = self.dictionary_class()
        else:
            self.dictionary = self.dictionary_class.load(self.path)
    
    @staticmethod
    def _get_file_path():
        for file_name in os.listdir(DictionaryManager.dir_path):
            full_path = os.path.join(DictionaryManager.dir_path, file_name)
            if os.path.isfile(full_path) and file_name.endswith(".dict"):
                return full_path
        return None
    
    def get_file_path(self):
        if (file_path := self._get_file_path()) is not None:
            self.path = file_path
    
    def check_dictionary_file(self):
        return self.get_file_path() is not None
    
    def digest(self, texts):
        self.dictionary.add_documents(texts)
    
    def save(self):
        self.dictionary.save(self.path)
    
    def get_token_to_id_map(self):
        return self.dictionary.token2id
    
    # Takes List[List[str]]  as input
    def get_corpus(self, texts: Iterable) -> Iterable:
        self.digest(texts)
        return [self.dictionary.doc2bow(text) for text in texts]


class TextAnalyzer:
    
    corpus_manager = CorpusManager()
    dictionary_manager = DictionaryManager()
    text_processor = TextProcessor()
    lda_model: LdaModel = None
    topics_manager = FrozenTopicInterface()
    dir_path = os.path.abspath(dirname(__file__))
    model_path = os.path.join(dir_path, "lda_model.bin")
    commitable_articles = []

    CHUNKSIZE = 100
    PASSES = 2 
    ALPHA = 'auto'
    ETA = 'auto'
    ITERATIONS= 50

    def load_modal(self):
        if os.path.exists(self.model_path):
            self.lda_model = LdaModel.load(self.model_path)
        else:
            self.lda_model = LdaModel(
                num_topics=self.topics_manager.num_topics(),
                chunksize=self.CHUNKSIZE,
                passes=self.PASSES,
                alpha=self.ALPHA,
                eta=self.ETA,
                per_word_topics=True,
                iterations=self.ITERATIONS
            )
        if self.lda_model.num_topics != self.topics_manager.num_topics():
            self.re_train = True

    def save_modal(self):
        if self.lda_model is not None:
            self.lda_model.save(self.model_path)
    
    def __init__(self, re_train: bool = False) -> None:
        self.re_train = re_train

    def load_corpus(self) -> CorpusHolder:
        # TODO: Batching in history loading
        corpus = []
        for file_path in self.corpus_manager.get_corpus_file_paths():
            corpus.append(self.corpus_manager._load_corpus(file_path))
        return CorpusHolder(corpus)
    

    def create_article(self, topic, meta: TextMeta):
        pass

    def flush_articles(self):
        self.commitable_articles = []

    def remap_topic_target(self):
        pass

    def commit_article_creation(self):
        pass
            
    
    def process_articles(self) -> CorpusHolder:
        articles = []
        metas = []
        for article in ArticleContainer.fetch_trainable_articles():
            texts, meta = self.text_processor._process_article(article)
            articles.append(texts)
            metas.append(meta)
        return CorpusHolder(
            corpus=self.dictionary_manager.get_corpus(articles),
            meta=metas
        )


    def corpus_generator(self) -> CorpusHolder:
        if self.re_train and self.corpus_manager.check_corpus_directory():
            return self.load_corpus()
        return self.process_articles()
    
    def train(self):
        self.load_modal()
        if self.re_train:
            self.lda_model.clear()
        corpus_holder = self.corpus_generator()
        self.lda_model.update(corpus_holder.corpus)






