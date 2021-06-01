from typing import Union
from pymongo.cursor import Cursor
from sqlalchemy.engine import Result

UniversalCursor = Union[Cursor, Result]

class CursorWrapper:
    """Wrapper for Universal Wrapper"""
    def __init__(self, cursor, db) -> None:
        self.cursor: UniversalCursor = cursor
        self.db = db
    
  