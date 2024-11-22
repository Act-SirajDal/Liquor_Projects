import json
import os

import pymysql
import scrapy
from scrapy.cmdline import execute
import metrobg.db_config as db
from metrobg.items import MetrobglinkItem


class LinkexpractSpider(scrapy.Spider):
    name = "linkexpract"
    allowed_domains = ["shop.metro.bg"]

    def __init__(self, name=None, start=0, end=0, **kwargs):
        super().__init__(name, **kwargs)
        # DATABASE SPECIFIC VALUES
        self.start = int(start)
        self.end = int(end)
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password)
        self.cursor = self.con.cursor()

    def start_requests(self):
        # update_query = f"UPDATE {db.db_links_table} SET Status='Pending'"
        # self.cursor.execute(update_query)
        # self.con.commit()
        url = 'https://shop.metro.bg/searchdiscover/articlesearch/search?storeId=00012&language=en-US&country=BG&query=*&rows=500&page=1&filter=category%3A%D1%85%D1%80%D0%B0%D0%BD%D0%B8%D1%82%D0%B5%D0%BB%D0%BD%D0%B8-%D1%81%D1%82%D0%BE%D0%BA%D0%B8%2F%D0%BD%D0%B0%D0%BF%D0%B8%D1%82%D0%BA%D0%B8%2F%D0%B2%D0%B8%D1%81%D0%BE%D0%BA%D0%BE%D0%B0%D0%BB%D0%BA%D0%BE%D1%85%D0%BE%D0%BB%D0%BD%D0%B8-%D0%BD%D0%B0%D0%BF%D0%B8%D1%82%D0%BA%D0%B8&facets=true&categories=true&__t=1693851830190'
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            # 'Cookie': 'selectedLocale_BG=bg-BG; anonymousUserId=5FC8CC74-9FF2-456A-BE08-D304DE9CB3C0; allowedCookieCategories=necessary%7Cfunctional%7Cperformance%7Cpromotional%7Cprofiling%7CUncategorized; _ga=GA1.2.1241858038.1703575520; local_ga=GA1.1.1241858038.1703575520; _gcl_au=1.1.600837547.1711967381; _ga_4N83E9ZK99=GS1.1.1711967470.5.1.1711969989.60.0.0; BIGipServerbetty.metrosystems.net-80=!HuO45t+g9U/5ieGSi9YRiDzMaGpIISbNc9MgTi/fUcbHJFV6ewzMhlh4Frdnu1/YyTh+EkuzkHdyijc=; abGroups={%22CI_ARTICLE_BANNER%22:%22B%22%2C%22QUICKENTRY_SEARCH_BACKEND%22:%22A%22%2C%22CI_USE_SEARCH_INSTEAD_OF_SIMPLESEARCH%22:%22A%22}; tsession={%22sessionId%22:%2290AC18E4-4526-4A0E-B66C-A1D48F43E503%22%2C%22timestamp%22:1716810057000}; _gid=GA1.2.987965711.1716810061; _fbp=fb.1.1716810217973.1751294157; local_ga_4N83E9ZK99=GS1.1.1716812299.7.1.1716812891.5.0.0; _dd_s=rum=0&expire=1716813805063',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }


        pl_file_path = fr"{db.PAGESAVE}/product_listing.html"
        meta_dict = {"pl_file_path":pl_file_path}
        if os.path.exists(pl_file_path):
            print('file:///' + pl_file_path)
            yield scrapy.Request(url='file:///' + pl_file_path,cb_kwargs=meta_dict,callback=self.parse,dont_filter=True)
        else:
            yield scrapy.Request(url=url,headers=headers,cb_kwargs=meta_dict,callback=self.parse,dont_filter=True)

    def parse(self,response,**kwargs):
        print("Parse Calling.....")
        pl_file_path = kwargs['pl_file_path']
        item = MetrobglinkItem()
        data = json.loads(response.text)['resultIds']
        if data:
            if not os.path.exists(pl_file_path):
                product_id_name = "product_listing"
                db.pagesave(response,product_id_name)
            else:
                print("File Already Available...")
            for ids in data:
                item['product_id'] = ids.strip('0032')
                item['url '] = f"https://shop.metro.bg/shop/pv/{item['product_id']}/0032/0021"
                yield item
        else:
            print("No any data found......")

if __name__ == '__main__':
    execute('scrapy crawl linkexpract'.split())
