from dataclasses import dataclass
from typing import List, Dict, Optional, Union


@dataclass
class DatabaseConfiguration:
    service: str
    host: str
    port: int
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    driver: Optional[str] = None

    def get_driver(self) -> str:
        return f"+{self.driver}" if len(self.driver) > 0 else ''
    
    def get_service(self) -> str:
        return self.service
    
    def get_database(self) -> str:
        return self.database
    
    def _get_user(self, in_str=True) -> Union[str, None]:
        if self.username == None and in_str:
            return ''
        return self.username
    
    def _get_password(self, in_str=True) -> Union[str, None]:
        if self.password == None and in_str:
            return ''
        return ":"+self.password
    
    def get_auth(self) -> str:
        return  

    def parse(self) -> str:
        return f"{self.service}{self.get_driver()}://{self._get_user()}{self._get_password()}@{self.host}:{self.port}/{self.database}"
    
    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)









