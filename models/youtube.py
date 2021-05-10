from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime

from sqlalchemy.sql.sqltypes import Enum
from .main import ArticleContainer, ContentType


@dataclass
class YoutubeVideoCategory:
    etag: str
    id: str
    title: str
    assignable: bool

    @classmethod
    def from_dict(cls, json: dict):
        cls.etag = json["etag"]
        cls.id = json['id']
        cls.title = json['snippet']['title']
        cls.assignable = json['snippet']['assignable']
        return cls
    
    @staticmethod
    def from_bulk(ls_json: List[dict]):
        return [YoutubeVideoCategory.from_dict(json) for json in ls_json]


class ThumbnailKeys(Enum):
    Default = "default"
    Medium = "medium",
    High = "high"
    Standard = "standard"

@dataclass
class YouTubeVideoModel:
    etag: str
    id: str
    pub_date: datetime
    channel_id: Optional[str]
    description: str
    thumbnails: Dict
    channel_title: str
    title: str
    category_id: Optional[str]
    duration_str: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    view_count: Optional[int] = field(default=0)
    like_count: Optional[int] = field(default=0)
    dislike_count: Optional[int] = field(default=0)
    comment_count: Optional[int] = field(default=0)

    video_category = None
    
    @property
    def link(self) -> str:
        return f"https://youtu.be/{self.id}"
    
    @property
    def channel_link(self) -> str:
        return f"https://www.youtube.com/channel/{self.channel_id}"
    
    @property
    def duration_secs(self) -> int:
        return self.get_duration_in_secs(self.duration_str)
    
    @property
    def article_id(self) -> str:
        return f"youtube-{self.id}"
    
    @staticmethod
    def get_duration_in_secs(duration_str) -> int:
        secs = 0
        nums = ['0','1','2','3','4','5','6','7','8','9']
        digit = ""
        for char in duration_str:
            if char == "D":
                secs += int(digit) * (60*60*24)
                digit = ""
            elif char == "H":
                secs += int(digit) * (60*60)
                digit = ""
            elif char == "M":
                secs += int(digit) * 60
                digit = ""
            elif char == "S":
                secs += int(digit)
                digit = ""
            elif char in nums:
                digit += char
        return secs
    
    def __post_init__(self):
        if self.duration_str is not None:
            self.duration_secs = self.get_duration_in_secs(self.duration_str)
    
    @classmethod
    def from_dict(cls, json, video_category: YoutubeVideoCategory = None, channel_id = None):
        data = {}
        data['etag'] = json['etag']
        data['id'] = json['id'] if isinstance(json['id'], str) else json['id']['videoId']
        data['pub_date'] = datetime.strptime(json['snippet']['publishedAt'], "%Y-%m-%dT%H:%M:%SZ")
        data['channel_id'] = channel_id
        if "channel_id" in json['snippet']:
            data['channel_id'] = json['snippet']['channel_id']
        data['title'] = json['snippet']['title']
        data['description'] = json['snippet']['description']
        data['thumbnails'] = json['snippet']['thumbnails']
        data['channel_title'] = json['snippet']['channelTitle']
        if "tags" in json['snippet']:
            data['tags'] = json['snippet']['tags']
        data['category_id'] = json['categoryId'] if 'categoryId' in json else None
        if "contentDetails" in json:
            data['duration_str'] = json['contentDetails']['duration']
        if "statistics" in json:
            data['view_count'] = int(json['statistics']['viewCount'])
            data['like_count'] = int(json['statistics']['likeCount'])
            data['dislike_count'] = int(json['statistics']['dislikeCount'])
            data['comment_count'] = int(json['statistics']['commentCount'])
        model = cls(**data)
        model.video_category = video_category
        return model
    
    def to_article(self) -> ArticleContainer:
        return ArticleContainer(
            article_id=self.article_id,
            title=self.title,
            creator=self.channel_title,
            article_link=self.link,
            source_name="Youtube",
            source_id="youtube-api",
            home_link=self.channel_link,
            site_name="Youtube",
            pub_date=self.pub_date,
            scraped_on=datetime.now(),
            text_set=[self.description] + self.tags,
            content=None,
            images=self.thumbnails[ThumbnailKeys.Standard],
            videos=[self.link],
            tags=self.tags,
            compulsory_tags=[self.video_category.title],
            is_source_present_in_db=True,
            majority_content_type=ContentType.Video,
            meta={
                "view_count": self.view_count,
                "like_count": self.like_count,
                "dislike_count": self.dislike_count,
                "comment_count": self.comment_count,
                "duration": self.duration_secs,
                "category_id": self.category_id,
                "etag": self.etag,
                "video_id": self.id,
                "channel_id": self.channel_id
            }
        )

    @staticmethod
    def from_bulk(ls_json: List[dict], video_category: YoutubeVideoCategory = None):
        return [YouTubeVideoModel.from_dict(json, video_category=video_category) for json in ls_json]
            


