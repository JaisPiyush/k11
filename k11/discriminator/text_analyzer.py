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


class TextAnalysisAdapter:
    corpus_manager = CorpusManager()
    dictionary_manager = DictionaryManager()
    text_processor = TextProcessor()
    model = None
    topics_manager = FrozenTopicInterface()
    dir_path = os.path.abspath(dirname(__file__))
    commitable_articles = []
    re_train = False

    @property
    def model_path(self):
        raise NotImplementedError("model_path property must be implemented.")
    
    def load_model(self):
        raise NotImplementedError("load_modal is not Implemeneted")
    
    def update_model(self, data, chunksize = 100):
        raise NotImplementedError("update_model method is not implemented")
    
    def save_model(self):
        raise NotImplementedError("save_model is not implemented.")
    
    def load_corpus(self) -> Generator[CorpusHolder, None, None]:
        for file_path in self.corpus_manager.get_corpus_file_paths():
            yield CorpusHolder(self.corpus_manager._load_corpus(file_path))
    
    def create_article(self, topic, meta: TextMeta):
        raise Exception("create_article has no code to run.")

    def flush_articles(self):
        self.commitable_articles = []

    def remap_topic_target(self):
        raise Exception("remap_topic_target has no code to run.")

    def commit_article_creation(self):
        raise Exception("commit_article_creation has no code to run. ")

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
    
    def corpus_generator(self) -> Generator[CorpusHolder]:
        if self.re_train and self.corpus_manager.check_corpus_directory():
            return self.load_corpus()
        yield self.process_articles()
    
    def _save_model(self):
        self.dictionary_manager.save()
    



class TextAnalyzer(TextAnalysisAdapter):
    
    CHUNKSIZE = 100
    PASSES = 2 
    ALPHA = 'auto'
    ETA = 'auto'
    ITERATIONS= 50

    @property
    def model_path(self):
        return os.path.join(self.dir_path, self.model_file_name)

    def update_model(self, data, chunksize = 100):
        self.model.update(data, chunksize=chunksize)

    def load_model(self):
        if os.path.exists(self.model_path):
            self.model = LdaModel.load(self.model_path)
        else:
            self.model = LdaModel(
                num_topics=self.topics_manager.num_topics(),
                chunksize=self.CHUNKSIZE,
                passes=self.PASSES,
                alpha=self.ALPHA,
                eta=self.ETA,
                per_word_topics=True,
                iterations=self.ITERATIONS
            )
        if self.model.num_topics != self.topics_manager.num_topics():
            self.re_train = True

    def save_model(self):
        self._save_model()
        if self.model is not None:
            self.model.save(self.model_path)
    
    def __init__(self, re_train: bool = False) -> None:
        self.re_train = re_train

    
    def train(self):
        self.load_model()
        if self.re_train:
            self.model.clear()
        for corpus_holder in self.corpus_generator():
            self.update_model(corpus_holder)
        self.save_model()






