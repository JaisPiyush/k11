from models.main import ThirdPartyDigger
from typing import Dict
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor, defer
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from .spiders import *
from .youtube.app import YoutubeDigger
import sched, time



class DiggerPilot:

    sched = None
    event = None

    def __init__(self) -> None:
        # self.spiders = {0: RSSFeedSpider, 1: HTMLFeedSpider, 2: HTMLArticleSpider}
        self.third_party_diggers: Dict[int, ThirdPartyDigger] = {0:YoutubeDigger()}

    def log(self, e: Exception):
        pass
    
    def run_spiders(self):
        configure_logging()
        self.runner = CrawlerRunner()
        @defer.inlineCallbacks
        def crawl():
            try:
                yield self.runner.crawl(RSSFeedSpider)
            except Exception as e:
                self.log(e)
            try:
                yield self.runner.crawl(HTMLFeedSpider)
            except Exception as e:
                self.log(e)
            try:
                yield self.runner.crawl(HTMLArticleSpider)
            except Exception as e:
                self.log(e)
            reactor.stop()
        crawl()
        reactor.run()
        
        
    
    def run_thrid_party_digger(self):
        for digger_index in sorted(self.third_party_diggers, key=lambda k:k, reverse=False):
            try:
                self.third_party_diggers[digger_index].run(log=self.log)
            except Exception as excp:
                self.log(excp)
            
    def start(self):
        self.run_thrid_party_digger()
        self.run_spiders()
    
    def init_scheduler(self):
        self.sched = sched.scheduler(time.time, time.sleep)

    def _schedule(self, time: int, priority=1):
        def _run():
            self.start()
            self.event = self.sched.enter(time, priority, _run,())
        self.event = self.sched.enter(0, priority, _run,())
    
    def scheduled(self, time:int, priority=1):
        self._schedule(time, priority=priority)
    
    def close_schedule(self):
        if self.sched is not None and self.event is not None:
            self.sched.cancel(self.event)


    


