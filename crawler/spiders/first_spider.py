# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
from crawler.items import CrawlerItem

import pymysql

from bs4 import BeautifulSoup
from time import sleep

class FirstSpider(CrawlSpider):
    name = "first"
    start_urls = [
        "http://www.clien.net",
    ]
    request_url = ''

    counter = 0
    sleep_counter = 1

    allowed_domains = [
        "clien.net",
        "daum.net",
        "naver.com",
        "ko.wikipedia.org",
        "tistory.com",
        "kr",
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
        "br.gov",
        "texashistory.unt.edu",
        "amazon.com",
        "indiatimes.com",
        "youtube.com",
        "phonearena.com",
    ]

    rules = (
        Rule(
            LinkExtractor(canonicalize=True,
                          unique=True,
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
        db_charset = self.settings.get('DB_CHARSET')

        self.conn = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            passwd=db_pass,
            database=db_db
        )

        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

        request_url = self.fetch_one_url(self.request_url)
        yield scrapy.Request(request_url, callback=self.parse, dont_filter=True)

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
        item['status'] = response.status
        item['raw'] = None
        item['is_visited'] = 'Y'
        item['rvrsd_domain'] = self.get_rvrsd_domain(response.request.meta.get('download_slot'))

        if response.status == 200:
            item['parsed'] = self.parse_text(response.text)
        else:
            item['parsed'] = None

        print('Success to parse: %s' % response.url)
        yield item

        links = LinkExtractor(canonicalize=True, unique=True, deny_domains=self.denied_domains).extract_links(response)
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

        parsed = soup.get_text().replace('\n', '').replace('\t', '').replace('\r', '')
        return parsed


    def get_rvrsd_domain(self, domain):
        splitList = domain.split('.')
        splitList.reverse()
        return ".".join(splitList)

    def fetch_one_url(self, request_url):
        print(request_url)
        sql = """
            SELECT url FROM DOC WHERE is_visited = 'N' and url <> %s and rvrsd_domain = 'kr.co.yonhapnews.www' limit 1;
            """
        self.cursor.execute(sql, (request_url))
        row = self.cursor.fetchone()

        if row == None:
            result = self.start_urls[0]
        else:
            result = row['url']

        return result
