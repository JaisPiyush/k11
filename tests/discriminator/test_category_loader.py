from typing import Callable
from k11.discriminator.datasets.category_load import get_categories


def test_get_categories():
    categories = get_categories()
    assert len(categories) == 613, "Categories aren't laoding"
    