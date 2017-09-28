# -*- coding: utf-8 -*-
import re
from time import sleep

import pymysql
import scrapy
from bs4 import BeautifulSoup

from crawler.items import CrawlerItem
from crawler.spiders.common_spider import CommonSpider


class EinformixSpider(CommonSpider):
    pattern = re.compile(r"[\n\r\t\0\s]+", re.DOTALL)
    name = "einformix"
    counter = 0

    def __init__(self, *a, **kw):
        print("Init einformix spider...")
        super(EinformixSpider, self).__init__(*a, **kw)

    def __del__(self):
        print("Finish einformix spider...")
        self.cursor.close()
        self.conn.close()

    def start_requests(self):
        db_host = self.settings.get('DB_HOST')
        db_port = self.settings.get('DB_PORT')
        db_user = self.settings.get('DB_USER')
        db_pass = self.settings.get('DB_PASS')
        db_db = self.settings.get('DB_DB')
        db_charset = self.settings.get('DB_CHARSET')

        self.conn = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            passwd=db_pass,
            database=db_db
        )

        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

        rows = self.fetch_urls_for_request()
        for row in rows:
           yield scrapy.Request(row['url'],
                                callback=self.parse,
                                errback=lambda x: self.download_errback(x, row['url']))

    def parse(self, response):

        item = CrawlerItem()
        item['url'] = response.url
        item['raw'] = None
        item['is_visited'] = 'Y'
        item['rvrsd_domain'] = self.get_rvrsd_domain(response.request.meta.get('download_slot'))

        try:
            item['status'] = response.status
            raw = response.text
            if response.status == 200:
                item['parsed'] = self.parse_text(raw)
            else:
                item['parsed'] = None

            self.counter = self.counter + 1
            if self.counter % 100 == 0:
                print('[%d] Sleep...' % self.counter)
                sleep(1)

            print('[%d] Parsed: %s' % (self.counter, response.url))

        except AttributeError as e:
            item['status'] = -3
            item['parsed'] = None
            self.logger.error('Fail to Parse: %s , because %s' % (response.url, e))
            print('[%d] Fail to Parse: %s , because %s' % (self.counter, response.url, e))

        return item

    def parse_text(self, raw):
        soup = BeautifulSoup(raw, "lxml")

        for surplus in soup(["script", "style"]):
            surplus.extract()

        try:
            article = soup.find("td", {"id": "articleBody"}).get_text()

            parsed = re.sub(self.pattern, " ", article, 0).replace('↑', '').replace('\'', '')
        except AttributeError as e:
            raise e

        return parsed


    def fetch_urls_for_request(self):
        sql = """
            SELECT url FROM DOC where RVRSD_DOMAIN='kr.co.einfomax.news' and url like 'http://news.einfomax.co.kr/news/articleView.html%' limit 500000
            """
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()

        return rows
