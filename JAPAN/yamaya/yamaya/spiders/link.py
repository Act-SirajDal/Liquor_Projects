import re
import pymysql
import scrapy
from scrapy.cmdline import execute
import yamaya.db_config as db
from yamaya.items import YamayaLink


class LinkSpider(scrapy.Spider):
    name = "link"
    allowed_domains = ["drive.yamaya.jp"]

    def __init__(self, start=0, end=0,name=None, **kwargs):
        super().__init__(name, **kwargs)
        self.start = start
        self.end = end
        self.con = pymysql.connect(host= db.db_host, user=db.db_user, password=db.db_password)
        self.cursor = self.con.cursor(pymysql.cursors.DictCursor)

    def start_requests(self):
        urls = ["https://drive.yamaya.jp/40417/catalog/list.php?CLASS=26&page=1#h-result", "https://drive.yamaya.jp/40417/catalog/list.php?CLASS=24#h-result"]

        for url in urls:
            yield scrapy.Request(url=url)

    def parse(self, response):
        item = YamayaLink()

        links = response.xpath('//div[@class="card"]//b/a/@href').getall()
        for link in links:
            item['url'] = response.urljoin(link)
            item['product_id'] = re.findall(r'CDSHIN=(\d+)', link)[0]

            yield item

        next_page = response.xpath('//li[@class="page-item "]//a[contains(text(), ">") and not(contains(text(), ">>"))]/@href').get()
        if next_page:
            yield scrapy.Request(url=response.urljoin(next_page))


if __name__ == '__main__':
    execute("scrapy crawl link".split())