# -*- coding: utf-8 -*-
import scrapy
from scrapy.exceptions import IgnoreRequest
from scrapy.linkextractors import LinkExtractor
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.spiders import Rule, CrawlSpider
from twisted.python import failure

from crawler.items import CrawlerItem

import pymysql

from bs4 import BeautifulSoup
from time import sleep

class TestSpider(CrawlSpider):
    name = "test"

    start_urls = [
        "http://www.clien.net",
        "http://shopping.naver.com/search/all.nhn?frm=NVSCTAB&query=%EC%B8%B5%EA%B3%BC+%EC%82%AC%EC%9D%B4&where=all",
    ]

    def __init__(self, *a, **kw):
        super(TestSpider, self).__init__(*a, **kw)


    def start_requests(self):
        db_host = self.settings.get('DB_HOST')
        db_port = self.settings.get('DB_PORT')
        db_user = self.settings.get('DB_USER')
        db_pass = self.settings.get('DB_PASS')
        db_db = self.settings.get('DB_DB')
        db_charset = self.settings.get('DB_CHARSET')

        # print(db_host)
        # print(db_port)
        # print(db_user)
        # print(db_pass)
        # print(db_db)
        # print(db_charset)
        self.conn = pymysql.connect(host='localhost', port=3306, user='work', passwd='work!@#', database='DOC')
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        url = self.start_urls[1]
        yield scrapy.Request(url,
                             callback=self.parse,
                             dont_filter=True,
                             errback=lambda x: self.download_errback(x, url))
    def __del__(self):
        self.cursor.close()
        self.conn.close()


    def parse(self, response):

        #print("Existing settings in parse: %s" % self.settings.attributes.keys())
        pass

    def download_errback(self, failure, url):

        if failure.check(IgnoreRequest):
            self.logger.debug('Forbidden by robot rule')
            item = CrawlerItem()
            item['url'] = url
            item['is_visited'] = 'Y'
            item['rvrsd_domain'] = None
            item['raw'] = None
            item['parsed'] = None
            item['status'] = -1

            yield item
        else:
            pass
