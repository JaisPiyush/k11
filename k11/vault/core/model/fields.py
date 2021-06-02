from typing import List, Dict
from datetime import datetime

class BaseField:
    type_: None
    value: None

    def __init__(self, primary_key:bool = False, server_default=None, unique:bool = False, index:bool = False, **kwargs) -> None:
        self.kwargs = {"primary_key": primary_key, "unique": unique, "index": index}
        if server_default:
            self.kwargs["server_default"] = server_default
        self.kwargs.update(kwargs)
    
    def set_type(self, type_):
        self.type_ = type_
    
    def set_value(self, value_):
        self.value = value_

    
    @classmethod
    def __get_validators__(cls):
        yield cls.validate
    
    @classmethod
    def validate(cls, v):
        print(cls)
        if not isinstance(v, cls.type_):
            raise TypeError(type(cls.type_) + " is required")
        cls.value = v
        return cls


    
    def get_field(self):
        """This method converts Field into respsective database formats"""
        raise NotImplementedError("get_field() method is missing")
    
    def get_value(self):
        """This method retreives value stored in class"""
        raise NotImplementedError("get_value() method must be implemented")


class String(BaseField):
    type_ = str

    def __init__(self,length=None, collation=None,convert_unicode:bool=False, unicode_error=None, _warn_on_bytestring:bool=False, _expect_unicode:bool=False, **kwargs) -> None:
        self.length = length
        super().__init__(**kwargs)

class Integer(BaseField):
    type_ = int
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

class Float(BaseField):
    type_ = float
    def __init__(self, precision=None, asdecimal=False, decimal_return_scale=None, **kwargs) -> None:
        super().__init__(**kwargs)


class DateTime(BaseField):
    type_ = datetime
    pass

class Array(BaseField):
    type_ = list
    def __init__(item_type, as_tupe:bool = False, dimensions=None, zero_indexes:bool=False,**kwargs):
        super().__init__(**kwargs)

class Record(BaseField):
    type_ = dict