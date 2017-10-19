# -*- coding: utf-8 -*-
# DEPRECATED
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

class FirstSpider(CrawlSpider):
    pattern = re.compile(r"[\n\r\t\0\s]+", re.DOTALL)
    name = "first"
    start_urls = [
        "https://www.clien.net/service/",
        "https://namu.wiki/w/%EC%8B%9D%EB%AC%BC%20vs%20%EC%A2%80%EB%B9%84%20%ED%9E%88%EC%96%B4%EB%A1%9C%EC%A6%88/%EC%A2%80%EB%B9%84/%EA%B5%90%ED%99%9C",
    ]
    request_url = ''

    counter = 0
    sleep_counter = 1

    denied_regex = [
        ".*mall.*",
        ".*search.*",
        ".*shop.*",
        ".*ko\.wikipedia\.org/w/.*",
        ".*\/board\/sold/.*",
        ".*\/board\/rule.*",
        ".*\/board\/notice.*",
        ".*\/board\/faq.*",
        ".*\/cs\/.*",
        ".*\/auth\/.*",
        ".*\/tag\/.*",
        ".*market.*",
        ".*moneta.*",
        ".*LostMgr\.php",
        ".*LostMgr\.php",
    ]

    allowed_domains = [
        "clien.net",
        "daum.net",
        "naver.com",
        "ko.wikipedia.org",
        "tistory.com",
        "kr",
        "namu.wiki",
        "www.yonhapnews.co.kr"
    ]

    denied_domains = [
        "twitter.com",
        "facebook.com",
        "instagram.com",
        "google.com",
        "archive.org",
        "bbc.co.uk",
        "commonswikimedia.org",
        "reuters.com",
        "wikibooks.org",
        "wikimedia.org",
        "wikinews.org",
        "mediawiki.org",
        "wikivoyage.org",
        "wikiquote.org",
        "wikidata.org",
        "wikisource.org",
        "wikiversity.org",
        "wiktionary.org",
        "wikimediafoundation.org",
        "reddit.com",
        "gov",
        "texashistory.unt.edu",
        "amazon.com",
        "indiatimes.com",
        "youtube.com",
        "phonearena.com",
        "s.ppomppu.co.kr",
        "saramin.co.kr",
        "go.kr",
        "moneta.co.kr",
        "hottracks.co.kr",
        "expedia.co.kr",
        "costco.co.kr",
        "movie.naver.com",
        "busan.koreapolice.co.kr",
        "www.korea.kr",
        "www.jacoup.co.kr",
        "or.kr",
        "www.ogage.co.kr",
    ]

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
        super(FirstSpider, self).__init__(*a, **kw)

    def __del__(self):
        print("Finish spider...")
        self.cursor.close()
        self.conn.close()

    def start_requests(self):
        db_host = self.settings.get('DB_HOST')
        db_port = self.settings.get('DB_PORT')
        db_user = self.settings.get('DB_USER')
        db_pass = self.settings.get('DB_PASS')
        db_db = self.settings.get('DB_DB')

        self.conn = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            passwd=db_pass,
            database=db_db
        )

        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

        request_url = self.fetch_one_url(self.request_url)
        # request_url = self.start_urls[0]
        yield scrapy.Request(request_url,
                             callback=self.parse,
                             errback=lambda x: self.download_errback(x, request_url))

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