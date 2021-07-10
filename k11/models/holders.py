from .main import SourceMap, LinkStore
from urllib.parse import urlparse

class SourceLinkView(object):
    def __init__(self, source_map: SourceMap, link_store: LinkStore):
        self.source_map = source_map
        self.link_store = link_store
        parsed_url = urlparse(link_store.link)
        self.link_url = f"{parsed_url.scheme}://{parsed_url.netloc}"