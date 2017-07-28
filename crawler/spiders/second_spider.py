# -*- coding: utf-8 -*-
import scrapy
from scrapy.exceptions import IgnoreRequest
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
from twisted.internet.error import DNSLookupError

from crawler.items import CrawlerItem

import pymysql

from bs4 import BeautifulSoup
from time import sleep

class SecondSpider(CrawlSpider):
    name = "second"
    counter = 0

    def __init__(self, *a, **kw):
        print("Init second spider...")
        super(SecondSpider, self).__init__(*a, **kw)

    def __del__(self):
        print("Finish for_parse_url spider...")
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

        # test_url = "https://www.sgic.co.kr/chp/fileDownload/download.mvc;jsessionid=vvVNjS05IjEVHy11OoAT3vje8KzvFySWceewEgDSb61DodNC9hDtAfGcWOdLaFI0.egisap2_servlet_engine13?fileId=014D8DBD1EFE5CD6629A629A"
        # yield scrapy.Request(test_url,
        #                      callback=self.parse,
        #                      errback=lambda x: self.download_errback(x, row['url']))

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
            self.logger.error(e)
            print('[%d] Fail to Parse: %s , because %s' % (self.counter, response.url, e))


        return item

    def parse_text(self, raw):
        soup = BeautifulSoup(raw, "lxml")

        for surplus in soup(["script", "style"]):
            surplus.extract()

        parsed = soup.get_text().replace('\n', '').replace('\t', '').replace('\r', '')
        return parsed


    def get_rvrsd_domain(self, domain):
        splitList = domain.split('.')
        splitList.reverse()
        return ".".join(splitList)

    def fetch_urls_for_request(self):
        sql = """
            SELECT url FROM DOC WHERE is_visited = 'N' limit 100000;
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

            yield item
        elif failure.check(DNSLookupError):
            self.logger.info('Fail to DNS lookup.')
            item['status'] = -2

            yield item
        else:
            pass