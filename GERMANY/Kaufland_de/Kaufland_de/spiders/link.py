# import json
# from Kaufland_de.items import *
#
# import scrapy
# from scrapy.cmdline import execute
#
#
# class LinkSpider(scrapy.Spider):
#     name = 'link'
#     allowed_domains = ['www.kaufland.de']
#     # start_urls = ['http://www.kaufland.de/']
#     # proxy = f"http://scraperapi:de51e4aafe704395654a32ba0a14494d:@proxy-server.scraperapi.com:8001"
#     proxy = 'http://9dbe950ef6284a5da9e7749db9f7cbd1:@api.zyte.com:8011'
#
#     def start_requests(self):
#
#         url = 'https://api.cloud.kaufland.de/search/v1/result-product-offers/?requestType=load&page=1&pageType=category_item_list&categoryId=2431&deviceType=desktop&loadType=pagination&useNewUrls=false&includeExtraAds=false'
#         # url = 'https://api.cloud.kaufland.de/search/v1/result-product-offers/?requestType=load&page=1&pageType=category_item_list&categoryId=69132&deviceType=desktop&loadType=pagination'
#
#         cookies = {
#             'AB-optimizely__device_type': 'desktop',
#             'AB-optimizely__browser_name': 'Chrome',
#             'AB-optimizely__environment': 'production',
#             '_fbp': 'fb.1.1703578452122.872523337',
#             'AB-optimizely_user': '6a7882e9-9624-45ef-8abe-c0300ba344c7',
#             '_cs_c': '0',
#             'x-storefront': 'de',
#             '_gcl_au': '1.1.1992774929.1721976618',
#             '_ga': 'GA1.1.1461653445.1703578451',
#             'FPAU': '1.1.1992774929.1721976618',
#             'api_ALTSESSID': 'qvg09pc4u738lk3sastg7foca4',
#             'hm_tracking': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NmUyN2Q5NzA0MWUzYTAwMDJjNTAyY2E5MzZiOGQ2ZWU1ZjJkZTk3MzRiOGJhOTM4MjJhYmFhMTI0NjJhY2RkZQ%3D%3D.CdY8ZSBZW2S2IEgfT%2Bvl%2Bu1qoZTrYCs5IkGJJCKUsWg%3D',
#             'api_hm_lsi': '347802039%2C375507937%2C360558084',
#             '_cs_mk': '0.7817395677195906_1724404819161',
#             '__cf_bm': 'DCh49jjh_W3dIRI1rED8m9dJ9bRhVk_prWr2daW.dbA-1724406379-1.0.1.1-loT5_eDUotnbQ11oxMxFG9e72K9fjecYb..RTBM5W3QwpTOQ7fWFR_p7vi1R44JpabPtnA_IKLUot4jSIPz5qg',
#             '_cs_id': '6e282815-d9fd-a9ef-8f8d-871fa8db1af4.1703578451.24.1724406382.1724404819.1713531742.1737742451453.1',
#             '_uetsid': 'e23d24a0612211ef865b57e28ae3e81a',
#             '_uetvid': '4434aae077ae11eeb6fc838426f35e84',
#             '_cs_s': '3.0.0.1724408182207',
#             '_ga_9WNMNEZ2M0': 'GS1.1.1724404819.37.1.1724406393.0.0.1148886034',
#         }
#
#         headers = {
#             'accept': 'application/json, text/plain, */*',
#             'accept-language': 'en-US,en;q=0.9',
#             # 'cookie': 'AB-optimizely__device_type=desktop; AB-optimizely__browser_name=Chrome; AB-optimizely__environment=production; _fbp=fb.1.1703578452122.872523337; AB-optimizely_user=6a7882e9-9624-45ef-8abe-c0300ba344c7; _cs_c=0; x-storefront=de; _gcl_au=1.1.1992774929.1721976618; _ga=GA1.1.1461653445.1703578451; FPAU=1.1.1992774929.1721976618; api_ALTSESSID=qvg09pc4u738lk3sastg7foca4; hm_tracking=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NmUyN2Q5NzA0MWUzYTAwMDJjNTAyY2E5MzZiOGQ2ZWU1ZjJkZTk3MzRiOGJhOTM4MjJhYmFhMTI0NjJhY2RkZQ%3D%3D.CdY8ZSBZW2S2IEgfT%2Bvl%2Bu1qoZTrYCs5IkGJJCKUsWg%3D; api_hm_lsi=347802039%2C375507937%2C360558084; _cs_mk=0.7817395677195906_1724404819161; __cf_bm=DCh49jjh_W3dIRI1rED8m9dJ9bRhVk_prWr2daW.dbA-1724406379-1.0.1.1-loT5_eDUotnbQ11oxMxFG9e72K9fjecYb..RTBM5W3QwpTOQ7fWFR_p7vi1R44JpabPtnA_IKLUot4jSIPz5qg; _cs_id=6e282815-d9fd-a9ef-8f8d-871fa8db1af4.1703578451.24.1724406382.1724404819.1713531742.1737742451453.1; _uetsid=e23d24a0612211ef865b57e28ae3e81a; _uetvid=4434aae077ae11eeb6fc838426f35e84; _cs_s=3.0.0.1724408182207; _ga_9WNMNEZ2M0=GS1.1.1724404819.37.1.1724406393.0.0.1148886034',
#             'origin': 'https://www.kaufland.de',
#             'priority': 'u=1, i',
#             'referer': 'https://www.kaufland.de/',
#             'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
#             'sec-ch-ua-mobile': '?0',
#             'sec-ch-ua-platform': '"Windows"',
#             'sec-fetch-dest': 'empty',
#             'sec-fetch-mode': 'cors',
#             'sec-fetch-site': 'same-site',
#             'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
#         }
#
#         params = {
#             'requestType': 'load',
#             'page': '5',
#             'pageType': 'category_item_list',
#             'categoryId': '69138',
#             'deviceType': 'desktop',
#             'loadType': 'pagination',
#             'useNewUrls': 'false',
#             'includeExtraAds': 'false',
#         }
#
#         yield scrapy.Request(url=url,
#                              callback=self.parse,
#                              headers=headers,
#                              cookies=cookies,
#                              meta={'count': 0,
#                                    'page': 1,
#                                    'proxy': self.proxy
#                                    })
#
#     def parse(self, response):
#         item = KauflandItem()
#         data = json.loads(response.text)
#         count = response.meta['count']
#         page = response.meta['page']
#         print(page)
#
#         product_urls = data['products']
#         if product_urls:
#             for i in product_urls:
#                 Product_id = i['id']
#                 link = i['link']['url']
#                 Platform_URL = f'https://www.kaufland.de/product/{Product_id}'
#                 # Product_id = link.split('product/')[1].split('/')[0]
#                 item['url'] = Platform_URL
#                 item['Product_id'] = Product_id
#                 yield item
#                 count += 1
#                 # print(Platform_URL)
#
#             total = data['page']['totalItemCount']
#
#             # if count < total:
#             page += 1
#             # print(page)
#             url = f'https://api.cloud.kaufland.de/search/v1/result-product-offers/?requestType=load&page={page}&pageType=category_item_list&categoryId=69132&deviceType=desktop&loadType=pagination&useNewUrls=false&includeExtraAds=false'
#             # url = f'https://api.cloud.kaufland.de/search/v1/result-product-offers/?requestType=load&page={page}&pageType=category_item_list&categoryId=69132&deviceType=desktop&loadType=pagination'
#             cookies = {
#                 'AB-optimizely__device_type': 'desktop',
#                 'AB-optimizely__browser_name': 'Chrome',
#                 'AB-optimizely__environment': 'production',
#                 '_fbp': 'fb.1.1703578452122.872523337',
#                 'AB-optimizely_user': '6a7882e9-9624-45ef-8abe-c0300ba344c7',
#                 '_cs_c': '0',
#                 'x-storefront': 'de',
#                 '_gcl_au': '1.1.1992774929.1721976618',
#                 '_ga': 'GA1.1.1461653445.1703578451',
#                 'FPAU': '1.1.1992774929.1721976618',
#                 'api_ALTSESSID': 'qvg09pc4u738lk3sastg7foca4',
#                 '_cs_mk': '0.36100765280970837_1724398791352',
#                 'hm_tracking': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NmUyN2Q5NzA0MWUzYTAwMDJjNTAyY2E5MzZiOGQ2ZWU1ZjJkZTk3MzRiOGJhOTM4MjJhYmFhMTI0NjJhY2RkZQ%3D%3D.CdY8ZSBZW2S2IEgfT%2Bvl%2Bu1qoZTrYCs5IkGJJCKUsWg%3D',
#                 'api_hm_lsi': '360558084',
#                 'SQsession': '1724399215629rr1lr',
#                 '__cf_bm': 'm.13lvzH1spePF9pYnEkcOhjDZPyQ0S4uXzqwIptyG0-1724399315-1.0.1.1-ZuJdBmpkvHADiaJh0lEQIX1yJeiPjzUNMTS_NxfPwPNt9fHhcVt0.y7DXbaVxmj4ygPJSm0_oSfIlg8fLIA_DQ',
#                 '_cs_id': '6e282815-d9fd-a9ef-8f8d-871fa8db1af4.1703578451.23.1724399392.1724398792.1713531742.1737742451453.1',
#                 '_uetsid': 'e23d24a0612211ef865b57e28ae3e81a',
#                 '_uetvid': '4434aae077ae11eeb6fc838426f35e84',
#                 '_cs_s': '27.0.0.1724401192536',
#                 '_ga_9WNMNEZ2M0': 'GS1.1.1724398791.36.1.1724399398.0.0.70632126',
#             }
#
#             headers = {
#                 'accept': 'application/json, text/plain, */*',
#                 'accept-language': 'en-US,en;q=0.9',
#                 # 'cookie': 'AB-optimizely__device_type=desktop; AB-optimizely__browser_name=Chrome; AB-optimizely__environment=production; _fbp=fb.1.1703578452122.872523337; AB-optimizely_user=6a7882e9-9624-45ef-8abe-c0300ba344c7; _cs_c=0; x-storefront=de; _gcl_au=1.1.1992774929.1721976618; _ga=GA1.1.1461653445.1703578451; FPAU=1.1.1992774929.1721976618; api_ALTSESSID=qvg09pc4u738lk3sastg7foca4; _cs_mk=0.36100765280970837_1724398791352; hm_tracking=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NmUyN2Q5NzA0MWUzYTAwMDJjNTAyY2E5MzZiOGQ2ZWU1ZjJkZTk3MzRiOGJhOTM4MjJhYmFhMTI0NjJhY2RkZQ%3D%3D.CdY8ZSBZW2S2IEgfT%2Bvl%2Bu1qoZTrYCs5IkGJJCKUsWg%3D; api_hm_lsi=360558084; SQsession=1724399215629rr1lr; __cf_bm=m.13lvzH1spePF9pYnEkcOhjDZPyQ0S4uXzqwIptyG0-1724399315-1.0.1.1-ZuJdBmpkvHADiaJh0lEQIX1yJeiPjzUNMTS_NxfPwPNt9fHhcVt0.y7DXbaVxmj4ygPJSm0_oSfIlg8fLIA_DQ; _cs_id=6e282815-d9fd-a9ef-8f8d-871fa8db1af4.1703578451.23.1724399392.1724398792.1713531742.1737742451453.1; _uetsid=e23d24a0612211ef865b57e28ae3e81a; _uetvid=4434aae077ae11eeb6fc838426f35e84; _cs_s=27.0.0.1724401192536; _ga_9WNMNEZ2M0=GS1.1.1724398791.36.1.1724399398.0.0.70632126',
#                 'origin': 'https://www.kaufland.de',
#                 'priority': 'u=1, i',
#                 # 'referer': 'https://www.kaufland.de/',
#                 'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
#                 'sec-ch-ua-mobile': '?0',
#                 'sec-ch-ua-platform': '"Windows"',
#                 'sec-fetch-dest': 'empty',
#                 'sec-fetch-mode': 'cors',
#                 'sec-fetch-site': 'same-site',
#                 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
#             }
#
#             yield scrapy.Request(url=url,
#                                  headers=headers,
#                                  cookies=cookies,
#                                  callback=self.parse,
#                                  dont_filter=True,
#                                  meta={'count': count,
#                                        'page': page,
#                                        'proxy': self.proxy})
#         else:
#             print("PAGINATION OVER")
#
#
# if __name__ == '__main__':
#     execute("scrapy crawl link".split())
