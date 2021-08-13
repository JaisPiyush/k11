from .models import DataLinkContainer, ArticleContainer, SourceMap
from ._serializer import Serializer


SourceMapSerializer = Serializer(fields=SourceMap._db_field_map.keys())
ArticleContainerSerializer = Serializer(fields=ArticleContainer._db_field_map.keys())
DataLinkSerializer = Serializer(fields=DataLinkContainer._db_field_map.keys())
