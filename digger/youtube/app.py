from models.youtube import YouTubeVideoModel, YoutubeVideoCategory
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