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

from crawler.spiders.common_spider import CommonSpider


class UrlCrawlSpider(CommonSpider):
    name = "url_crawler"
    start_urls = [
        "https://www.clien.net/service/",
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
        ".*schedule.*",
        ".*channel.*",
        ".*%ED%8A%B9%EC%88%98:.*",
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
        print("Init UrlCrawlSpider...")
        super(UrlCrawlSpider, self).__init__(*a, **kw)

    def __del__(self):
        print("Finish UrlCrawlSpider...")
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
