from dataclasses import dataclass
from typing import Generator, Optional, List
import pytest
from ..main import Format, SourceMap


def test_pull_rss_models():
    source_maps: Generator[SourceMap,None, None] = SourceMap.pull_all_rss_models()
    for source_map in source_maps:
        assert(source_map.is_rss == True and source_map.is_collection == True)
    




