# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):
    url = scrapy.Field()
    raw = scrapy.Field()
    is_visited = scrapy.Field()
    parsed = scrapy.Field()
    rvrsd_domain = scrapy.Field()
    status = scrapy.Field()
    pass
