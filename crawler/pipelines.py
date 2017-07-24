# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
import pymysql
import hashlib

from scrapy.exceptions import DropItem


class CrawlerPipeline(object):
    def __init__(self, my_settings):
        self.settings = my_settings
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
            database=db_db,
            use_unicode=True,
            charset=db_charset)
        self.cursor = self.conn.cursor()

    @classmethod
    def from_crawler(cls, crawler):
        my_settings = crawler.settings
        return cls(my_settings)

    def process_item(self, item, spider):
        url = item['url']
        id = self.get_doc_id(url)
        is_visited = item['is_visited'] is None and 'N' or item['is_visited']
        raw = item['raw']
        parsed = item['parsed']
        rvrsd_domain = item['rvrsd_domain']
        status = item['status']

        if is_visited == "N":
            sql = """
				INSERT INTO DOC (id, c_time, url, is_visited, rvrsd_domain, visit_cnt)
				SELECT %s, now(), %s, %s, %s, 0 FROM DUAL
				WHERE NOT EXISTS (SELECT * FROM DOC WHERE id=%s)
				"""
            self.cursor.execute(sql, (id, url, is_visited, rvrsd_domain, id))
            print("Save new URL: %s" % url)
        elif is_visited == "Y":
            sql = """
				INSERT INTO DOC (id, c_time, v_time, raw, parsed, url, is_visited, rvrsd_domain)
				VALUES (%s, now(), now(), %s, %s, %s, %s, %s)
				ON DUPLICATE KEY UPDATE raw = %s, is_visited = %s, parsed = %s, v_time = now(), visit_cnt = visit_cnt + 1, status = %s
				"""
            self.cursor.execute(sql, (id, raw, parsed, url, is_visited, rvrsd_domain, raw, is_visited, parsed, status))
            print("Update URL: %s" % url)
        else:
            print("Pass URL: %s" % url)
            pass

        self.conn.commit()

        return item

    def get_doc_id(self, url):
        return hashlib.md5(url.encode('utf-8')).hexdigest()[0:16]

    def open_spider(self, spider):
        print('Open Spider...')

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()
        print('Close Spider...')
