from models.youtube import YouTubeVideoModel, YoutubeVideoCategory
from models.main import ArticleContainer, SourceMap, ThirdPartyDigger
import json
from typing import Generator, List
import requests

class YoutubeApi:
    def __init__(self, key=None) -> None:
        self.key = key
        self.url = "https://www.googleapis.com/youtube/v3/"
    
    def _create_request(self, path: str, **kwargs) -> str:
        url = self.url + path + "?"
        for key, value in kwargs.items():
            if value is not None and len(value) > 0:
                url += f"{key}={value}&"
        url += f"key={self.key}"
        return url
    
    def request_server(self, url) -> requests.Response:
        return requests.get(url)
    

    def fetch_videos(self, url: str) -> List[YouTubeVideoModel]:
        try:
            response = self.request_server(url)
            data = json.loads(response.content)
            if "items" in data:
                return YouTubeVideoModel.from_bulk(data['items'])
            else:
                return None
        except Exception as e:
            raise e

    
    def fetch_video_categories(self, part='snippet', region_code="IN") -> List[YoutubeVideoCategory]:
        url = self._create_request('videoCategories',part=part, regionCode=region_code )
        try:
            response = self.request_server(url)
            data = json.loads(response.content)
            if "items" in data:
                return YoutubeVideoCategory.from_bulk(data['items'])
            else:
                return None
        except Exception as e:
            return None
    
    def fetch_video_using_category(self, category: YoutubeVideoCategory, part="snippet,contentDetals,statistics", **kwargs ) -> List[YouTubeVideoModel]:
        kwargs['videoCategiryId'] = category.id
        url = self._create_request("videos",part=part, **kwargs)
        return self.fetch_videos(url)
    
    def fetch_all_videos_of_channel(self,channel_id: str, part="snippet", **kwargs) -> List[YouTubeVideoModel]:
        kwargs['channelId'] = channel_id
        url =self._create_request("search", part=part, **kwargs)
        return self.fetch_videos(url)
    
    def fetch_trending_video_of_category(self,category: YoutubeVideoCategory, region_code="IN", max_results=10) -> List[YouTubeVideoModel]:
        return self.fetch_video_using_category(category, chart="mostPopular", maxResults=max_results, regionCode=region_code)
    
    def fetch_all_trending_videos(self) -> Generator[List[YouTubeVideoModel], None, None]:
        for category in self.fetch_video_categories():
            yield self.fetch_trending_video_of_category(category)


class YoutubeDigger(ThirdPartyDigger):

    def __init__(self) -> None:
        super().__init__()
        self.yt_api = YoutubeApi(key="AIzaSyBVEJjoudsni8HTaZ7nEUZZmb5Wb9MHUC4")
    
    @staticmethod
    def is_video_present_in_db(article_id:str) -> bool:
        return ArticleContainer.adapter().find_one({"article_id": article_id}, silent=True) != None

    
    def store_articles_in_db(self, videos: List[YouTubeVideoModel]):
        for video in videos:
            if not self.is_video_present_in_db(video.article_id):
                ArticleContainer.adapter().create(**(video.to_article()).to_dict())
    
    def store_all_trending_videos(self):
        for videos in self.yt_api.fetch_all_trending_videos():
            self.store_articles_in_db(videos)
    
    def pull_source_from_database(self) -> Generator[SourceMap, None, None]:
        return SourceMap.pull_all_youtube_collections()
    
    def store_all_channel_videos(self):
        for source in self.pull_source_from_database():
            for link in source.links:
                self.store_articles_in_db(self.yt_api.fetch_all_videos_of_channel(link.link))
        
    
    def run(self, **kwargs):
        try:
            self.store_all_trending_videos()
            self.store_all_channel_videos()
        except Exception as e:
            if "log" in kwargs:
                kwargs['log'](e)
