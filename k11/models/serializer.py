
from k11.models import DataLinkContainer, ArticleContainer

class Serializer:
    def __init__(self, fields=[]) -> None:
        self.fields = fields
    
    def serialize(self, model):
        return {key: getattr(model, key) for key in self.fields}
    
    def __call__(self, model) -> dict:
        return self.serialize(model)


DataLinkSerializer = Serializer(fields=DataLinkContainer._db_field_map.keys())
ArticleContainerSerializer = Serializer(fields=ArticleContainer._db_field_map.keys())