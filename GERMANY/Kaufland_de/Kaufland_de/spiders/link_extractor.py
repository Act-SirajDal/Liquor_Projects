import json
from Kaufland_de.items import *
import scrapy
from scrapy.cmdline import execute
import Kaufland_de.db_config as db
import pymysql
import os
from Kaufland_de.pipelines import KauflandDePipeline
import urllib.parse

class LinkSpider(scrapy.Spider):
    name = 'link'
    handle_httpstatus_list = [403, 401]
    allowed_domains = ['www.kaufland.de']
    # start_urls = ['http://www.kaufland.de/']
    # proxy = f"http://scraperapi:de51e4aafe704395654a32ba0a14494d:@proxy-server.scraperapi.com:8001"
    proxy = 'http://9dbe950ef6284a5da9e7749db9f7cbd1:@api.zyte.com:8011'


    def __init__(self, name=None, start=0, end=0, **kwargs):
        super().__init__(name, **kwargs)
        # DATABASE SPECIFIC VALUES
        self.start = int(start)
        self.end = int(end)
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password,database=db.db_name)
        self.cursor = self.con.cursor()

    def start_requests(self):
        update_query = f'''UPDATE {db.db_links_table} set status="Pending"'''
        try:
            self.cursor.execute(update_query)
            self.con.commit()
        except Exception as e:
            print(e)
        category_ids = ['2431','69132','69138','69157'] # ['Spirituosen','Wodka','Rum','Whiskey']
        for category_id in category_ids:
            # url = 'https://api.cloud.kaufland.de/search/v1/result-product-offers/?requestType=load&page=1&pageType=category_item_list&categoryId=2431&deviceType=desktop&loadType=pagination&useNewUrls=false&includeExtraAds=false'
            url = 'https://api.cloud.kaufland.de/search/v1/result-product-offers/?requestType=load&page=1&pageType=category_item_list&categoryId=69132&deviceType=desktop&loadType=pagination'
            cookies = {
                '__cf_bm': 'JkxthjLsm9NCBjVqCF.MZnakaGz2PrVBMQyB8qX4b2U-1727850329-1.0.1.1-gQ2raMCp4N0vceYxnx7FRXYO2WbUusaGSn8bzhP8N1EVi598R5885Uux49G4UVfSlucfI24xrvmaEM_qoRw27g',
            }
            # cookies = {
            #     'AB-optimizely__device_type': 'desktop',
            #     'AB-optimizely__browser_name': 'Chrome',
            #     'AB-optimizely__environment': 'production',
            #     '_fbp': 'fb.1.1703578452122.872523337',
            #     'AB-optimizely_user': '6a7882e9-9624-45ef-8abe-c0300ba344c7',
            #     '_cs_c': '0',
            #     'x-storefront': 'de',
            #     '_gcl_au': '1.1.1992774929.1721976618',
            #     '_ga': 'GA1.1.1461653445.1703578451',
            #     'FPAU': '1.1.1992774929.1721976618',
            #     'api_ALTSESSID': 'qvg09pc4u738lk3sastg7foca4',
            #     'hm_tracking': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NmUyN2Q5NzA0MWUzYTAwMDJjNTAyY2E5MzZiOGQ2ZWU1ZjJkZTk3MzRiOGJhOTM4MjJhYmFhMTI0NjJhY2RkZQ%3D%3D.CdY8ZSBZW2S2IEgfT%2Bvl%2Bu1qoZTrYCs5IkGJJCKUsWg%3D',
            #     'api_hm_lsi': '347802039%2C375507937%2C360558084',
            #     '_cs_mk': '0.7817395677195906_1724404819161',
            #     '__cf_bm': 'DCh49jjh_W3dIRI1rED8m9dJ9bRhVk_prWr2daW.dbA-1724406379-1.0.1.1-loT5_eDUotnbQ11oxMxFG9e72K9fjecYb..RTBM5W3QwpTOQ7fWFR_p7vi1R44JpabPtnA_IKLUot4jSIPz5qg',
            #     '_cs_id': '6e282815-d9fd-a9ef-8f8d-871fa8db1af4.1703578451.24.1724406382.1724404819.1713531742.1737742451453.1',
            #     '_uetsid': 'e23d24a0612211ef865b57e28ae3e81a',
            #     '_uetvid': '4434aae077ae11eeb6fc838426f35e84',
            #     '_cs_s': '3.0.0.1724408182207',
            #     '_ga_9WNMNEZ2M0': 'GS1.1.1724404819.37.1.1724406393.0.0.1148886034',
            # }
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'max-age=0',
                # 'cookie': '__cf_bm=JkxthjLsm9NCBjVqCF.MZnakaGz2PrVBMQyB8qX4b2U-1727850329-1.0.1.1-gQ2raMCp4N0vceYxnx7FRXYO2WbUusaGSn8bzhP8N1EVi598R5885Uux49G4UVfSlucfI24xrvmaEM_qoRw27g',
                'if-none-match': 'W/"15ab5-1NMBF7TPK281hyxDnZdvUrp3g6A"',
                'priority': 'u=0, i',
                'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'none',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            }
            # headers = {
            #     'accept': 'application/json, text/plain, */*',
            #     'accept-language': 'en-US,en;q=0.9',
            #     # 'cookie': 'AB-optimizely__device_type=desktop; AB-optimizely__browser_name=Chrome; AB-optimizely__environment=production; _fbp=fb.1.1703578452122.872523337; AB-optimizely_user=6a7882e9-9624-45ef-8abe-c0300ba344c7; _cs_c=0; x-storefront=de; _gcl_au=1.1.1992774929.1721976618; _ga=GA1.1.1461653445.1703578451; FPAU=1.1.1992774929.1721976618; api_ALTSESSID=qvg09pc4u738lk3sastg7foca4; hm_tracking=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NmUyN2Q5NzA0MWUzYTAwMDJjNTAyY2E5MzZiOGQ2ZWU1ZjJkZTk3MzRiOGJhOTM4MjJhYmFhMTI0NjJhY2RkZQ%3D%3D.CdY8ZSBZW2S2IEgfT%2Bvl%2Bu1qoZTrYCs5IkGJJCKUsWg%3D; api_hm_lsi=347802039%2C375507937%2C360558084; _cs_mk=0.7817395677195906_1724404819161; __cf_bm=DCh49jjh_W3dIRI1rED8m9dJ9bRhVk_prWr2daW.dbA-1724406379-1.0.1.1-loT5_eDUotnbQ11oxMxFG9e72K9fjecYb..RTBM5W3QwpTOQ7fWFR_p7vi1R44JpabPtnA_IKLUot4jSIPz5qg; _cs_id=6e282815-d9fd-a9ef-8f8d-871fa8db1af4.1703578451.24.1724406382.1724404819.1713531742.1737742451453.1; _uetsid=e23d24a0612211ef865b57e28ae3e81a; _uetvid=4434aae077ae11eeb6fc838426f35e84; _cs_s=3.0.0.1724408182207; _ga_9WNMNEZ2M0=GS1.1.1724404819.37.1.1724406393.0.0.1148886034',
            #     'origin': 'https://www.kaufland.de',
            #     'priority': 'u=1, i',
            #     'referer': 'https://www.kaufland.de/',
            #     'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            #     'sec-ch-ua-mobile': '?0',
            #     'sec-ch-ua-platform': '"Windows"',
            #     'sec-fetch-dest': 'empty',
            #     'sec-fetch-mode': 'cors',
            #     'sec-fetch-site': 'same-site',
            #     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            # }

            page = 1
            params = {
                'requestType': 'load',
                'page': f'{page}',
                'pageType': 'category_item_list',
                'categoryId': category_id,
                'deviceType': 'desktop',
                'loadType': 'pagination',
                'useNewUrls': 'false',
                'includeExtraAds': 'false',
            }

            base_url = 'https://api.cloud.kaufland.de/search/v1/result-product-offers/'

            # Encode the parameters to a query string
            query_string = urllib.parse.urlencode(params)

            # Construct the full URL
            url = f"{base_url}?{query_string}"


            pl_file_path = fr"{db.PAGESAVE}/Kaufland_de_PL_{page}_{category_id}.html"
            meta_dict = {"pl_file_path": pl_file_path, "count": 0, "proxy": self.proxy, "page": page,"category_id":category_id}
            if os.path.exists(pl_file_path):
                print('file:///' + pl_file_path)
                yield scrapy.Request(url='file:///' + pl_file_path,cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
            else:
                yield scrapy.Request(url=url,callback=self.parse,headers=headers,cookies=cookies,cb_kwargs=meta_dict,meta={'proxy': self.proxy},dont_filter=True)

    def parse(self, response,**kwargs):
        print(response.text)
        item = KauflandItem()
        data = json.loads(response.text)
        count = kwargs['count']
        page = kwargs['page']
        category_id = kwargs['category_id']
        pl_file_path = fr"{db.PAGESAVE}/Kaufland_de_PL_{page}_{category_id}.html"
        print(page)

        product_urls = data['products']
        if product_urls:
            if not os.path.exists(pl_file_path):
                product_id_name = f"Kaufland_de_PL_{page}_{category_id}"
                db.pagesave(response, product_id_name)
            else:
                print("File Already Available...")

            for i in product_urls:
                Product_id = i['id']
                link = i['link']['url']
                Platform_URL = f'https://www.kaufland.de/product/{Product_id}'
                # Product_id = link.split('product/')[1].split('/')[0]
                item['url'] = Platform_URL
                item['Product_id'] = Product_id
                yield item
                count += 1
                # print(Platform_URL)

            total = data['page']['totalItemCount']

            if count < total:
                page += 1
                # print(page)
                next_url = f'https://api.cloud.kaufland.de/search/v1/result-product-offers/?requestType=load&page={page}&pageType=category_item_list&categoryId={category_id}&deviceType=desktop&loadType=pagination&useNewUrls=false&includeExtraAds=false'
                print(next_url)
                # url = f'https://api.cloud.kaufland.de/search/v1/result-product-offers/?requestType=load&page={page}&pageType=category_item_list&categoryId=69132&deviceType=desktop&loadType=pagination'
                cookies = {
                    'AB-optimizely__device_type': 'desktop',
                    'AB-optimizely__browser_name': 'Chrome',
                    'AB-optimizely__environment': 'production',
                    '_fbp': 'fb.1.1703578452122.872523337',
                    'AB-optimizely_user': '6a7882e9-9624-45ef-8abe-c0300ba344c7',
                    '_cs_c': '0',
                    'x-storefront': 'de',
                    '_gcl_au': '1.1.1992774929.1721976618',
                    '_ga': 'GA1.1.1461653445.1703578451',
                    'FPAU': '1.1.1992774929.1721976618',
                    'api_ALTSESSID': 'qvg09pc4u738lk3sastg7foca4',
                    '_cs_mk': '0.36100765280970837_1724398791352',
                    'hm_tracking': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NmUyN2Q5NzA0MWUzYTAwMDJjNTAyY2E5MzZiOGQ2ZWU1ZjJkZTk3MzRiOGJhOTM4MjJhYmFhMTI0NjJhY2RkZQ%3D%3D.CdY8ZSBZW2S2IEgfT%2Bvl%2Bu1qoZTrYCs5IkGJJCKUsWg%3D',
                    'api_hm_lsi': '360558084',
                    'SQsession': '1724399215629rr1lr',
                    '__cf_bm': 'm.13lvzH1spePF9pYnEkcOhjDZPyQ0S4uXzqwIptyG0-1724399315-1.0.1.1-ZuJdBmpkvHADiaJh0lEQIX1yJeiPjzUNMTS_NxfPwPNt9fHhcVt0.y7DXbaVxmj4ygPJSm0_oSfIlg8fLIA_DQ',
                    '_cs_id': '6e282815-d9fd-a9ef-8f8d-871fa8db1af4.1703578451.23.1724399392.1724398792.1713531742.1737742451453.1',
                    '_uetsid': 'e23d24a0612211ef865b57e28ae3e81a',
                    '_uetvid': '4434aae077ae11eeb6fc838426f35e84',
                    '_cs_s': '27.0.0.1724401192536',
                    '_ga_9WNMNEZ2M0': 'GS1.1.1724398791.36.1.1724399398.0.0.70632126',
                }

                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'accept-language': 'en-US,en;q=0.9',
                    # 'cookie': 'AB-optimizely__device_type=desktop; AB-optimizely__browser_name=Chrome; AB-optimizely__environment=production; _fbp=fb.1.1703578452122.872523337; AB-optimizely_user=6a7882e9-9624-45ef-8abe-c0300ba344c7; _cs_c=0; x-storefront=de; _gcl_au=1.1.1992774929.1721976618; _ga=GA1.1.1461653445.1703578451; FPAU=1.1.1992774929.1721976618; api_ALTSESSID=qvg09pc4u738lk3sastg7foca4; _cs_mk=0.36100765280970837_1724398791352; hm_tracking=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NmUyN2Q5NzA0MWUzYTAwMDJjNTAyY2E5MzZiOGQ2ZWU1ZjJkZTk3MzRiOGJhOTM4MjJhYmFhMTI0NjJhY2RkZQ%3D%3D.CdY8ZSBZW2S2IEgfT%2Bvl%2Bu1qoZTrYCs5IkGJJCKUsWg%3D; api_hm_lsi=360558084; SQsession=1724399215629rr1lr; __cf_bm=m.13lvzH1spePF9pYnEkcOhjDZPyQ0S4uXzqwIptyG0-1724399315-1.0.1.1-ZuJdBmpkvHADiaJh0lEQIX1yJeiPjzUNMTS_NxfPwPNt9fHhcVt0.y7DXbaVxmj4ygPJSm0_oSfIlg8fLIA_DQ; _cs_id=6e282815-d9fd-a9ef-8f8d-871fa8db1af4.1703578451.23.1724399392.1724398792.1713531742.1737742451453.1; _uetsid=e23d24a0612211ef865b57e28ae3e81a; _uetvid=4434aae077ae11eeb6fc838426f35e84; _cs_s=27.0.0.1724401192536; _ga_9WNMNEZ2M0=GS1.1.1724398791.36.1.1724399398.0.0.70632126',
                    'origin': 'https://www.kaufland.de',
                    'priority': 'u=1, i',
                    # 'referer': 'https://www.kaufland.de/',
                    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                }
                meta_dict = {"count": count,"page": page}
                pl_file_path = fr"{db.PAGESAVE}/Kaufland_de_PL_{page}_{category_id}.html"
                if os.path.exists(pl_file_path):
                    print('file:///' + pl_file_path)
                    yield scrapy.Request(url='file:///' + pl_file_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
                else:
                    yield scrapy.Request(url=next_url,headers=headers,cookies=cookies,callback=self.parse,dont_filter=True,cb_kwargs=meta_dict,meta={"proxy": self.proxy})
            else:
                print("PAGINATION OVER")


if __name__ == '__main__':
    execute("scrapy crawl link".split())
