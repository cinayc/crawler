# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
from crawler.items import CrawlerItem

import pymysql

from bs4 import BeautifulSoup
from time import sleep

class TestSpider(CrawlSpider):
    name = "test"

    start_urls = [
        "http://www.clien.net",
    ]

    def __init__(self, *a, **kw):
        print("Init spider...")

        super(TestSpider, self).__init__(*a, **kw)


    def start_requests(self):
        #print("Existing settings in start_requests: %s" % self.settings.attributes.keys())
        db_host = self.settings.get('DB_HOST')
        db_port = self.settings.get('DB_PORT')
        db_user = self.settings.get('DB_USER')
        db_pass = self.settings.get('DB_PASS')
        db_db = self.settings.get('DB_DB')
        db_charset = self.settings.get('DB_CHARSET')

        print(db_host)
        print(db_port)
        print(db_user)
        print(db_pass)
        print(db_db)
        print(db_charset)
        self.conn = pymysql.connect(host='localhost', port=3306, user='work', passwd='work!@#', database='DOC')
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

        yield scrapy.Request(self.start_urls[0], callback=self.parse, dont_filter=True)
    def __del__(self):
        print("Finish spider...")
        self.cursor.close()
        self.conn.close()


    def parse(self, response):

        #print("Existing settings in parse: %s" % self.settings.attributes.keys())
        pass
