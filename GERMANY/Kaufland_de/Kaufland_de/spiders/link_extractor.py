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
    proxy = f"http://scraperapi:de51e4aafe704395654a32ba0a14494d:@proxy-server.scraperapi.com:8001"
    # proxy = 'http://9dbe950ef6284a5da9e7749db9f7cbd1:@api.zyte.com:8011'


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
        # category_ids = ['2431','69132','69138','69157'] # ['Spirituosen','Wodka','Rum','Whiskey']
        category_ids = ['spirituosen','wodka','rum','whiskey']
        for category_id in category_ids:
            cookies = {
                'AB-optimizely_user': '38df72dd-1d54-451a-ba58-3c10a250211c',
                'AB-optimizely__device_type': 'desktop',
                'AB-optimizely__browser_name': 'Chrome',
                'AB-optimizely__environment': 'production',
                'ALTSESSID': 'jpn7q8pgm65jm50tiiaptsndd2',
                'OptanonAlertBoxClosed': '2024-10-02T07:57:31.480Z',
                'eupubconsent-v2': 'CQF2nyQQF2nyQAcABBENBJF4APLAAAAAAAYgF5wBAAKgAoABYAvMAAAAgSAEABUBeY6AEABUBeZKACAvMpACAAqAvMAA.flgAAAAAAAAA',
                '_gcl_au': '1.1.1729977423.1727855852',
                '_fbp': 'fb.1.1727855851866.767702107317593549',
                '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%228iR20Kh4li1Q6t1bnNRP%22%7D',
                '_ga': 'GA1.1.1925035489.1727855853',
                'axd': '4375020065483650037',
                'tis': '',
                'FPAU': '1.1.1729977423.1727855852',
                'api_ALTSESSID': 'jpn7q8pgm65jm50tiiaptsndd2',
                '__cf_bm': '3cuPt_cnC5Xex_SlxEaWkAVewYZ4S5WkOOm0IQP0M14-1732525360-1.0.1.1-9PHiu5qU6oEustJBADJuyvCFFfk0FVazaErPTKVNEzyEttCRXTWVwGw6eITBCVvyHbh0L.iXZ8JkUpa29OyyaA',
                'x-storefront': 'de',
                'x-country': 'DE',
                'storefront-selector-preferences': '[]',
                'cf_clearance': 'rpYTi9V6tEAvVA50WODtUF8MW5a9Vqf9dluO5zsB60A-1732525385-1.2.1.1-g7RjblFs4x0g3Gc3uwvG0idsvfh3_XmmXFQaHBorjb8ZX6g7vyNWhc3J0d1SWYrl0mfrQaz85KxdSWXEOraI2FA6oCjJ6XU_.nTZr3N1akxrE50mWnVWcWSD03u3C74yPHPclhE78R5DHjH7ym9YRUl6EwxenJSsvGVoVc4dsC0sK44YCzuwpB01mHQu4SLdlml.9_5nhSXphSv7onLE1HgRS7eOd0AT4T0y33slzWq3FgnN2Es_BEHtm5wnjDJEoT0yekz14dHCWbnhEdVX2RVmyHfzZdSwV_65MSdpZtaWWLSiaLi7FrtcMSKANv33JOZAJmREEll0qujr0hozkQc0Kh08r3FQGMLrL.gn2XcPx90U5y1EVHJJ5XF1qpUQayNzFXBe2I7ephXodewQ5PEO6Lrf3vZm2XQUMVLr1OPqeW7AcQ8nVNpsgRuqQiKC',
                'hm_tracking': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.MmU5ZDg1M2QwZjY3YjE5ZmYxOTkzNjlmOGNkNjIyOWEyYzY1YzIwNWM3ODNiYzkwNDY0NjA2ZTU0YzFhNTE0Ng%3D%3D.bEmlaXzfRN4cL%2F5r7PWss1b3kAwOUwYVfcyNCjbBfyI%3D',
                '_ttp': 'cwZxOBfl18hAUCjK6eQ5mjgnB1y',
                'kndctr_BCF65C6655685E857F000101_AdobeOrg_identity': 'CiY0ODY2NDA0ODY2MzgzOTc2NjI5MDM4MTk4MTc5Mjc4Nzc1MjM3OVIRCNndv5S2MhgBKgRJUkwxMAHwAdndv5S2Mg==',
                'kndctr_BCF65C6655685E857F000101_AdobeOrg_cluster': 'irl1',
                'AMCV_BCF65C6655685E857F000101%40AdobeOrg': 'MCMID|48664048663839766290381981792787752379',
                'mbox': 'session%2348664048663839766290381981792787752379%2DKpfDXH%231732527281',
                'mboxEdgeCluster': '37',
                'lea_utms': 'utm_source=stationary-de-header&utm_medium=referral&utm_campaign=stationary-de-header&utm_content=undefined',
                'OptanonConsent': 'isGpcEnabled=0&datestamp=Mon+Nov+25+2024+14%3A40%3A24+GMT%2B0530+(India+Standard+Time)&version=202410.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=d4d9c87c-f278-41dd-9bef-a9b22e5cbcdf&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CBG2530%3A1%2CC0030%3A1%2CBG2520%3A1%2CBG2487%3A1%2CBG2488%3A1%2CBG2489%3A1%2CBG2490%3A1%2CBG2491%3A1%2CBG2492%3A1%2CBG2493%3A1%2CC0053%3A1%2CC0049%3A1%2CC0054%3A1%2CC0041%3A1%2CC0047%3A1%2CC0055%3A1%2CC0072%3A0&geolocation=IN%3BGJ&AwaitingReconsent=false&isAnonUser=1',
                '_uetsid': '169e0840ab0c11ef961fc703dc109fa9',
                '_uetvid': 'fa6b99d0809311efa4d1a3be79b17001',
                '_dd_s': 'logs=0&expire=1732526772728&rum=0',
                '_ga_9WNMNEZ2M0': 'GS1.1.1732525387.4.1.1732525874.0.0.986990383',
            }

            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-US,en;q=0.9',
                # 'cookie': 'AB-optimizely_user=38df72dd-1d54-451a-ba58-3c10a250211c; AB-optimizely__device_type=desktop; AB-optimizely__browser_name=Chrome; AB-optimizely__environment=production; ALTSESSID=jpn7q8pgm65jm50tiiaptsndd2; OptanonAlertBoxClosed=2024-10-02T07:57:31.480Z; eupubconsent-v2=CQF2nyQQF2nyQAcABBENBJF4APLAAAAAAAYgF5wBAAKgAoABYAvMAAAAgSAEABUBeY6AEABUBeZKACAvMpACAAqAvMAA.flgAAAAAAAAA; _gcl_au=1.1.1729977423.1727855852; _fbp=fb.1.1727855851866.767702107317593549; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%228iR20Kh4li1Q6t1bnNRP%22%7D; _ga=GA1.1.1925035489.1727855853; axd=4375020065483650037; tis=; FPAU=1.1.1729977423.1727855852; api_ALTSESSID=jpn7q8pgm65jm50tiiaptsndd2; x-storefront=de; storefront-selector-preferences=[]; hm_tracking=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.MmU5ZDg1M2QwZjY3YjE5ZmYxOTkzNjlmOGNkNjIyOWEyYzY1YzIwNWM3ODNiYzkwNDY0NjA2ZTU0YzFhNTE0Ng%3D%3D.bEmlaXzfRN4cL%2F5r7PWss1b3kAwOUwYVfcyNCjbBfyI%3D; _ttp=cwZxOBfl18hAUCjK6eQ5mjgnB1y; kndctr_BCF65C6655685E857F000101_AdobeOrg_identity=CiY0ODY2NDA0ODY2MzgzOTc2NjI5MDM4MTk4MTc5Mjc4Nzc1MjM3OVIRCNndv5S2MhgBKgRJUkwxMAHwAdndv5S2Mg==; AMCV_BCF65C6655685E857F000101%40AdobeOrg=MCMID|48664048663839766290381981792787752379; mbox=session%2348664048663839766290381981792787752379%2DKpfDXH%231732527281; lea_utms=utm_source=stationary-de-header&utm_medium=referral&utm_campaign=stationary-de-header&utm_content=undefined; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Nov+25+2024+14%3A40%3A24+GMT%2B0530+(India+Standard+Time)&version=202410.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=d4d9c87c-f278-41dd-9bef-a9b22e5cbcdf&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CBG2530%3A1%2CC0030%3A1%2CBG2520%3A1%2CBG2487%3A1%2CBG2488%3A1%2CBG2489%3A1%2CBG2490%3A1%2CBG2491%3A1%2CBG2492%3A1%2CBG2493%3A1%2CC0053%3A1%2CC0049%3A1%2CC0054%3A1%2CC0041%3A1%2CC0047%3A1%2CC0055%3A1%2CC0072%3A0&geolocation=IN%3BGJ&AwaitingReconsent=false&isAnonUser=1; _uetsid=169e0840ab0c11ef961fc703dc109fa9; _uetvid=fa6b99d0809311efa4d1a3be79b17001; _ga_9WNMNEZ2M0=GS1.1.1732525387.4.1.1732525882.0.0.986990383; cf_clearance=0fXM9Zry4P5ugbOpAIuBOneFHTGQmmdaFVHFlXn9x7c-1732528186-1.2.1.1-JTnGx1fOmKEn4Ez.KcGGnhJYFXqb10yfbmVvcIRxDPkg3qPLqXdvN2XirJG6OkK9roGTDBfoN4_hESHg59CqXyExLZwGLqNhcYGfr5fn5zkk7O857u9vOTY4RKU.YSzr_JbdxWTRo9HbyscdKShR_qETHx9eTkDRVm98qenRs0t_RC52EbGgtHJuXCh6gIx2RGsM5ARpd9TNWMsS08Y9_9GyzCmJqrg3JRo4gnmzTAjPVqUq07RHqFB2mUn2VMtOzZxmSljoixM9rFWtQ5wh2cZCV31Y4xL37X62n85A_rFKSztk0Miy7AAaAGaB8ZTzqwethxkvYTq2AOd_X8W9nw3FVTS5wVdz6MRgc0PkA9A4d.pey9bu6NaPLn7xr8wQgif0tQDx8qIiRY7Jtjr4Y8NfUX0ItH3CMC0DGn.CeAj56AZ_JThmqxMX5UbKtjXY; _dd_s=isExpired=1; x-country=DE; __cf_bm=b0sisrtnk8NntSOQ9MgiBFJNuGKBK3cY7s_wzE2fIic-1732528875-1.0.1.1-4BCmq_.hnOzFmFwD5GAZ_dTI4Lf9R76.g3JFnzBaKk4VDiWmycfRcjYAYczYRuIgLczwXEGyNfNkdYP8KxIARw',
                'priority': 'u=1, i',
                'referer': 'https://www.kaufland.de/s/?search_value=whisky',
                'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                'sec-ch-ua-arch': '"x86"',
                'sec-ch-ua-bitness': '"64"',
                'sec-ch-ua-full-version': '"131.0.6778.86"',
                'sec-ch-ua-full-version-list': '"Google Chrome";v="131.0.6778.86", "Chromium";v="131.0.6778.86", "Not_A Brand";v="24.0.0.0"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-model': '""',
                'sec-ch-ua-platform': '"Windows"',
                'sec-ch-ua-platform-version': '"15.0.0"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            }

            page = 1
            params = {
                'requestType': 'initial-load',
                'page':page,
                'pageType': 'search',
                'searchValue': category_id,
                'deviceType': 'desktop',
                'useNewUrls': 'true',
            }
            base_url = 'https://www.kaufland.de/api/search/v1/result-product-offers'

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
                # yield scrapy.Request(url=url,callback=self.parse,headers=headers,cookies=cookies,cb_kwargs=meta_dict,meta={'proxy': self.proxy},dont_filter=True)
                yield scrapy.Request(url=url,callback=self.parse,headers=headers,cookies=cookies,cb_kwargs=meta_dict,meta={'proxy': self.proxy},dont_filter=True)
            # break
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

                params = {
                    'requestType': 'load',
                    'page': page,
                    'pageType': 'search',
                    'searchValue': category_id,
                    'deviceType': 'desktop',
                    'loadType': 'pagination',
                    'useNewUrls': 'true',
                }
                base_url = 'https://www.kaufland.de/api/search/v1/result-product-offers'

                # Encode the parameters to a query string
                query_string = urllib.parse.urlencode(params)

                next_url = f"{base_url}?{query_string}"
                # print(page)

                # url = f'https://api.cloud.kaufland.de/search/v1/result-product-offers/?requestType=load&page={page}&pageType=category_item_list&categoryId=69132&deviceType=desktop&loadType=pagination'
                cookies = {
                    'X-Request-ID': '8e810347ee533ae3-BOM',
                    'AB-optimizely_user': '38df72dd-1d54-451a-ba58-3c10a250211c',
                    'AB-optimizely__device_type': 'desktop',
                    'AB-optimizely__browser_name': 'Chrome',
                    'AB-optimizely__environment': 'production',
                    'ALTSESSID': 'jpn7q8pgm65jm50tiiaptsndd2',
                    'OptanonAlertBoxClosed': '2024-10-02T07:57:31.480Z',
                    'eupubconsent-v2': 'CQF2nyQQF2nyQAcABBENBJF4APLAAAAAAAYgF5wBAAKgAoABYAvMAAAAgSAEABUBeY6AEABUBeZKACAvMpACAAqAvMAA.flgAAAAAAAAA',
                    '_gcl_au': '1.1.1729977423.1727855852',
                    '_fbp': 'fb.1.1727855851866.767702107317593549',
                    '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%228iR20Kh4li1Q6t1bnNRP%22%7D',
                    '_ga': 'GA1.1.1925035489.1727855853',
                    'axd': '4375020065483650037',
                    'tis': '',
                    'FPAU': '1.1.1729977423.1727855852',
                    'api_ALTSESSID': 'jpn7q8pgm65jm50tiiaptsndd2',
                    'x-storefront': 'de',
                    'storefront-selector-preferences': '[]',
                    'hm_tracking': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.MmU5ZDg1M2QwZjY3YjE5ZmYxOTkzNjlmOGNkNjIyOWEyYzY1YzIwNWM3ODNiYzkwNDY0NjA2ZTU0YzFhNTE0Ng%3D%3D.bEmlaXzfRN4cL%2F5r7PWss1b3kAwOUwYVfcyNCjbBfyI%3D',
                    '_ttp': 'cwZxOBfl18hAUCjK6eQ5mjgnB1y',
                    'kndctr_BCF65C6655685E857F000101_AdobeOrg_identity': 'CiY0ODY2NDA0ODY2MzgzOTc2NjI5MDM4MTk4MTc5Mjc4Nzc1MjM3OVIRCNndv5S2MhgBKgRJUkwxMAHwAdndv5S2Mg==',
                    'AMCV_BCF65C6655685E857F000101%40AdobeOrg': 'MCMID|48664048663839766290381981792787752379',
                    'mbox': 'session%2348664048663839766290381981792787752379%2DKpfDXH%231732527281',
                    'lea_utms': 'utm_source=stationary-de-header&utm_medium=referral&utm_campaign=stationary-de-header&utm_content=undefined',
                    'cf_chl_rc_i': '1',
                    'OptanonConsent': 'isGpcEnabled=0&datestamp=Mon+Nov+25+2024+16%3A09%3A06+GMT%2B0530+(India+Standard+Time)&version=202410.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=d4d9c87c-f278-41dd-9bef-a9b22e5cbcdf&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CBG2530%3A1%2CC0030%3A1%2CBG2520%3A1%2CBG2487%3A1%2CBG2488%3A1%2CBG2489%3A1%2CBG2490%3A1%2CBG2491%3A1%2CBG2492%3A1%2CBG2493%3A1%2CC0053%3A1%2CC0049%3A1%2CC0054%3A1%2CC0041%3A1%2CC0047%3A1%2CC0055%3A1%2CC0072%3A0&geolocation=IN%3BGJ&AwaitingReconsent=false&isAnonUser=1',
                    '_uetsid': '169e0840ab0c11ef961fc703dc109fa9',
                    '_uetvid': 'fa6b99d0809311efa4d1a3be79b17001',
                    'cf_clearance': '25HzdpIpnp3YLcFRTgkMepYgGhlEzqc88yecyx1axyQ-1732531162-1.2.1.1-0njcwUL4pvhzCwCXm3Jx4TH7novnvEGi_NGkySM0CBNQHETbdtyf5sDwTLGG4a4LbagGGIvukMaiKyngf1vLdCU6K6gmoQuJkdGOCzV3m6etJRFJ6xh.9gRzXqxnPuFV5lGrBHDamz72m25jU5ldJImcXdayMnNPrX4_33X4C551wl.MdVF53xbdNDo_.TSwPfOcJCAhXOUVVXAudfbJXoWJGecDEcDvKXlPjTBHUgFHZ_tOITU_NEq9e4nhWL19RQBgSU5E5x6H7qMVQSNb0t9MPX51Jzdyn1qochtcTkV85hPvUNAKdLnOFlrVLpQjeO_GzvgrPGKQZ_fKXvMia72xYr6LTMMnRkMM2H5A8bsWeGyOXWavwSCCxmAsQLmdCl.65GirFlEMO3n4cHSCGLjnc3PC1zmAdgzKXpGKbsRxCWL9_97nBH_tgIJVzd2K',
                    'x-country': 'DE',
                    '__cf_bm': 'd3omBi7kMvuiqg4U8Xz9DdWjE2KkkFYM_VmX0Vh2Fkk-1732532358-1.0.1.1-mQae0PXpRSmS1JvHn1U6C6VDErlBYRYcYRXy1scpbAdaBjUUiY4g1tMgai1sQhofXLt6vh1WDdUwOG0VIdweYw',
                    '_ga_9WNMNEZ2M0': 'GS1.1.1732531148.5.1.1732532371.0.0.621983263',
                    '_dd_s': 'logs=0&expire=1732533284086&rum=0',
                }

                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'accept-language': 'en-US,en;q=0.9',
                    # 'cookie': 'X-Request-ID=8e810347ee533ae3-BOM; AB-optimizely_user=38df72dd-1d54-451a-ba58-3c10a250211c; AB-optimizely__device_type=desktop; AB-optimizely__browser_name=Chrome; AB-optimizely__environment=production; ALTSESSID=jpn7q8pgm65jm50tiiaptsndd2; OptanonAlertBoxClosed=2024-10-02T07:57:31.480Z; eupubconsent-v2=CQF2nyQQF2nyQAcABBENBJF4APLAAAAAAAYgF5wBAAKgAoABYAvMAAAAgSAEABUBeY6AEABUBeZKACAvMpACAAqAvMAA.flgAAAAAAAAA; _gcl_au=1.1.1729977423.1727855852; _fbp=fb.1.1727855851866.767702107317593549; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%228iR20Kh4li1Q6t1bnNRP%22%7D; _ga=GA1.1.1925035489.1727855853; axd=4375020065483650037; tis=; FPAU=1.1.1729977423.1727855852; api_ALTSESSID=jpn7q8pgm65jm50tiiaptsndd2; x-storefront=de; storefront-selector-preferences=[]; hm_tracking=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.MmU5ZDg1M2QwZjY3YjE5ZmYxOTkzNjlmOGNkNjIyOWEyYzY1YzIwNWM3ODNiYzkwNDY0NjA2ZTU0YzFhNTE0Ng%3D%3D.bEmlaXzfRN4cL%2F5r7PWss1b3kAwOUwYVfcyNCjbBfyI%3D; _ttp=cwZxOBfl18hAUCjK6eQ5mjgnB1y; kndctr_BCF65C6655685E857F000101_AdobeOrg_identity=CiY0ODY2NDA0ODY2MzgzOTc2NjI5MDM4MTk4MTc5Mjc4Nzc1MjM3OVIRCNndv5S2MhgBKgRJUkwxMAHwAdndv5S2Mg==; AMCV_BCF65C6655685E857F000101%40AdobeOrg=MCMID|48664048663839766290381981792787752379; mbox=session%2348664048663839766290381981792787752379%2DKpfDXH%231732527281; lea_utms=utm_source=stationary-de-header&utm_medium=referral&utm_campaign=stationary-de-header&utm_content=undefined; cf_chl_rc_i=1; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Nov+25+2024+16%3A09%3A06+GMT%2B0530+(India+Standard+Time)&version=202410.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=d4d9c87c-f278-41dd-9bef-a9b22e5cbcdf&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CBG2530%3A1%2CC0030%3A1%2CBG2520%3A1%2CBG2487%3A1%2CBG2488%3A1%2CBG2489%3A1%2CBG2490%3A1%2CBG2491%3A1%2CBG2492%3A1%2CBG2493%3A1%2CC0053%3A1%2CC0049%3A1%2CC0054%3A1%2CC0041%3A1%2CC0047%3A1%2CC0055%3A1%2CC0072%3A0&geolocation=IN%3BGJ&AwaitingReconsent=false&isAnonUser=1; _uetsid=169e0840ab0c11ef961fc703dc109fa9; _uetvid=fa6b99d0809311efa4d1a3be79b17001; cf_clearance=25HzdpIpnp3YLcFRTgkMepYgGhlEzqc88yecyx1axyQ-1732531162-1.2.1.1-0njcwUL4pvhzCwCXm3Jx4TH7novnvEGi_NGkySM0CBNQHETbdtyf5sDwTLGG4a4LbagGGIvukMaiKyngf1vLdCU6K6gmoQuJkdGOCzV3m6etJRFJ6xh.9gRzXqxnPuFV5lGrBHDamz72m25jU5ldJImcXdayMnNPrX4_33X4C551wl.MdVF53xbdNDo_.TSwPfOcJCAhXOUVVXAudfbJXoWJGecDEcDvKXlPjTBHUgFHZ_tOITU_NEq9e4nhWL19RQBgSU5E5x6H7qMVQSNb0t9MPX51Jzdyn1qochtcTkV85hPvUNAKdLnOFlrVLpQjeO_GzvgrPGKQZ_fKXvMia72xYr6LTMMnRkMM2H5A8bsWeGyOXWavwSCCxmAsQLmdCl.65GirFlEMO3n4cHSCGLjnc3PC1zmAdgzKXpGKbsRxCWL9_97nBH_tgIJVzd2K; x-country=DE; __cf_bm=d3omBi7kMvuiqg4U8Xz9DdWjE2KkkFYM_VmX0Vh2Fkk-1732532358-1.0.1.1-mQae0PXpRSmS1JvHn1U6C6VDErlBYRYcYRXy1scpbAdaBjUUiY4g1tMgai1sQhofXLt6vh1WDdUwOG0VIdweYw; _ga_9WNMNEZ2M0=GS1.1.1732531148.5.1.1732532371.0.0.621983263; _dd_s=logs=0&expire=1732533284086&rum=0',
                    'priority': 'u=1, i',
                    'referer': 'https://www.kaufland.de/s/?search_value=whisky',
                    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                    'sec-ch-ua-arch': '"x86"',
                    'sec-ch-ua-bitness': '"64"',
                    'sec-ch-ua-full-version': '"131.0.6778.86"',
                    'sec-ch-ua-full-version-list': '"Google Chrome";v="131.0.6778.86", "Chromium";v="131.0.6778.86", "Not_A Brand";v="24.0.0.0"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-model': '""',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-ch-ua-platform-version': '"15.0.0"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
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
