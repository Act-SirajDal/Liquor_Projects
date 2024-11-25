import pymysql
import scrapy
from scrapy.cmdline import execute
from edeka24.items import Edeka24Links
import os
import edeka24.db_config as db


class LinkExtractorSpider(scrapy.Spider):
    name = "link_extractor"
    allowed_domains = ["edeka24.de"]
    start_urls = ["https://edeka24.de"]

    def __init__(self, name=None, start=0, end=0, **kwargs):
        super().__init__(name, **kwargs)
        # DATABASE SPECIFIC VALUES
        self.start = int(start)
        self.end = int(end)
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password)
        self.cursor = self.con.cursor()
        self.data_insert = 0
        scraper_api_key = 'de51e4aafe704395654a32ba0a14494d'
        self.scraper_api_url = f'http://api.scraperapi.com/?api_key={scraper_api_key}&url='

        self.cat_skip_list = []

        self.headers = {
            'authority': 'www.edeka24.de',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,pt;q=0.8,gu;q=0.7',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        }

    def start_requests(self):
        # update_query = f'''UPDATE {db.db_links_table} set status="Pending"'''
        # self.cursor.execute(update_query)
        # self.con.commit()
        cat_dict = {
                        'Braende': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Braende/',
                        'Brandy-Weinbrand': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Brandy-Weinbrand/',
                        'Cognac': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Cognac/',
                        'Gin': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Gin/',
                        'Grappa': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Grappa/',
                        'Likoere-Bitter': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Likoere-Bitter/',
                         'Met': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Met/',
                        'Party-Schnaps': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Party-Schnaps/',
                        # 'Rum': 'https://www.edeka24.de/Weinspirit_links-Spirituosen/Spirituosen/Rum/',
                        'Rum': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Rum/',
                        'Tequila': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Tequila/',
                        'Vodka': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Vodka/',
                        'Wermut': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Wermut/',
                        'Whisky': 'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Whisky/',
                        'pre-mix':'https://www.edeka24.de/Wein-Spirituosen/Spirituosen/Mixgetraenke-in-Dosen'

        }
        for cat in cat_dict:
            cat_name = cat
            page = 0
            link_count = 0
            cat_url = cat_dict[cat_name] + '?pgNr=0'
            pl_file_path = fr"{db.PAGESAVE}/{cat_name}_product_listing_{page+1}.html"
            meta_dict = {"pl_file_path": pl_file_path,"cat_name": cat_name,"cat_url": cat_url,"page": page,"link_count":link_count}
            if os.path.exists(pl_file_path):
                print('file:///' + pl_file_path)
                yield scrapy.Request(url='file:///' + pl_file_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
            else:
                yield scrapy.Request(url=cat_url,headers=self.headers,callback=self.parse,cb_kwargs=meta_dict,dont_filter=True)

    def parse(self,response,**kwargs):
        print(response.url)
        page = kwargs['page'] + 1
        cat_url = kwargs['cat_url']
        cat_url = cat_url.split('?pgNr=')[0].strip() + f'?pgNr={page}'
        cat_name = kwargs['cat_name']
        link_count = kwargs['link_count']
        pl_file_path = fr"{db.PAGESAVE}/{cat_name}_product_listing_{page}.html"
        total_product_count = int(response.xpath('//div[@class="category-amount-articles"]/span/text()').get())
        all_products = response.xpath('//*[@class="product-item"]')
        if all_products:
            if not os.path.exists(pl_file_path):
                product_id_name = f"{cat_name}_product_listing_{page}"
                db.pagesave(response, product_id_name)
            else:
                print("File Already Available...")


            for product in all_products:
                link_count += 1
                item = Edeka24Links()
                item['category'] = cat_name
                item['Product_id'] = product.xpath('.//a/@data-artobaconf_sarticlenumber[1]').get().strip()
                item['Product_URL'] = product.xpath('.//div[@class="product-image"]/a/@href').get().strip()

                yield item
            if link_count != total_product_count:
                meta_dict = {"cat_name": cat_name, "cat_url": cat_url, "page": page,"link_count":link_count}
                yield scrapy.Request(url=cat_url,headers=self.headers,callback=self.parse,cb_kwargs=meta_dict,dont_filter=True)


if __name__ == '__main__':
    execute('scrapy crawl link_extractor'.split())
