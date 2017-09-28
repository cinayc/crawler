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

class CommonSpider(CrawlSpider):
    pattern = re.compile(r"[\n\r\t\0\s]+", re.DOTALL)
    name = "common"
    start_urls = [
        "https://www.clien.net/service/",
    ]
    request_url = ''

    counter = 0
    sleep_counter = 1

    denied_regex = []

    allowed_domains = []

    denied_domains = []

    rules = (
        Rule(
            LinkExtractor(canonicalize=True,
                          unique=True,
                          deny=denied_regex,
                          allow_domains=allowed_domains,
                          deny_domains=denied_domains),
            callback="parse_link",
            follow=True),
    )

    def __init__(self, *a, **kw):
        print("Init spider...")
        super(CommonSpider, self).__init__(*a, **kw)

    def __del__(self):
        print("Finish spider...")
        self.cursor.close()
        self.conn.close()

    def parse_link(self, response):

        self.counter = self.counter + 1

        if self.counter % 100 == 0:
            self.sleep_counter = self.sleep_counter + 1
            print('Sleep...[%d]' % self.sleep_counter)
            sleep(1)

        if self.sleep_counter % 20 == 0:
            self.sleep_counter = self.sleep_counter + 1
            print('Deep sleep...[%d]' % self.sleep_counter)
            sleep(100)

        print('Try to parse: %s' % response.url)

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

            print('[%d] Success to parse: %s' % (self.counter, response.url))

        except AttributeError as e:
            item['status'] = -3
            item['parsed'] = None
            self.logger.error(e)
            print('[%d] Fail to Parse: %s , because %s' % (self.counter, response.url, e))

        yield item

        links = LinkExtractor(canonicalize=True, unique=True, deny=self.denied_regex, deny_domains=self.denied_domains).extract_links(response)
        if len(links) > 0:
            for link in links:
                linkItem = CrawlerItem()
                linkItem['url'] = link.url
                linkItem['status'] = None
                linkItem['raw'] = None
                linkItem['is_visited'] = 'N'
                linkItem['parsed'] = None
                linkItem['rvrsd_domain'] = self.get_rvrsd_domain(link.url.split('/')[2])

                yield linkItem

    def parse_text(self, raw):
        parsed = None
        soup = BeautifulSoup(raw, "lxml")

        for surplus in soup(["script", "style"]):
            surplus.extract()

        parsed = re.sub(self.pattern, " ", soup.get_text(), 0)
        return parsed

    def get_rvrsd_domain(self, domain):
        splitList = domain.split('.')
        splitList.reverse()
        return ".".join(splitList)

    def fetch_one_url(self, request_url):
        sql = """
            SELECT url FROM DOC WHERE is_visited = 'N' and url <> %s and rvrsd_domain = 'kr.co.yonhapnews.www' limit 10;
            """
        self.cursor.execute(sql, (request_url))
        row = self.cursor.fetchone()

        if row == None:
            result = self.start_urls[0]
        else:
            result = row['url']

        return result

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