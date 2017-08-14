# -*- coding: utf-8 -*-
import scrapy
from scrapy.exceptions import IgnoreRequest
from scrapy.linkextractors import LinkExtractor
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.spiders import Rule, CrawlSpider
from service_identity.exceptions import DNSMismatch
from twisted.internet.error import DNSLookupError, NoRouteError
from crawler.items import CrawlerItem
import pymysql
from bs4 import BeautifulSoup
from time import sleep
import re

class WikipediaSpider(CrawlSpider):
    pattern = re.compile(r"[\n\r\t\0\s]+", re.DOTALL)
    name = "wikipedia"
    counter = 0

    def __init__(self, *a, **kw):
        print("Init wikipedia spider...")
        super(WikipediaSpider, self).__init__(*a, **kw)

    def __del__(self):
        print("Finish wikipedia spider...")
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

        try:
            article = soup.find("div", {"class": "mw-parser-output"}).get_text()
            parsed = re.sub(self.pattern, " ", article, 0).replace('↑', '').replace('\'', '')
        except AttributeError as e:
            raise e

        return parsed


    def get_rvrsd_domain(self, domain):
        splitList = domain.split('.')
        splitList.reverse()
        return ".".join(splitList)

    def fetch_urls_for_request(self):
        sql = """
            SELECT url FROM DOC WHERE is_visited = 'N' and rvrsd_domain = 'org.wikipedia.ko' limit 100000
            """
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()

        return rows

    def download_errback(self, failure, url):
        item = CrawlerItem()
        item['url'] = url
        item['is_visited'] = 'Y'
        item['rvrsd_domain'] = None
        item['raw'] = None
        item['parsed'] = None

        if failure.check(IgnoreRequest):
            self.logger.debug('Forbidden by robot rule')
            item['status'] = -1

        elif failure.check(DNSLookupError):
            self.logger.info('Fail to DNS lookup.')
            item['status'] = -2

        elif failure.check(DNSMismatch):
            self.logger.info('Fail to DNS match.')
            item['status'] = -2

        elif failure.check(NoRouteError):
            self.logger.info('No route error.')
            item['status'] = -4

        elif failure.check(HttpError):
            status = failure.value.response
            self.logger.info('Http error [%s].' % status)
            item['status'] = status

        else:
            self.logger.info('Unknown error.')
            item['status'] = -255

        yield item