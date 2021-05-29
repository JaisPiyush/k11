from scrapy.exceptions import DropItem

class SkipItem(DropItem):
    pass

class DirectlyCreatedItem(DropItem):
    pass