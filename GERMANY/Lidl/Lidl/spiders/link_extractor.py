import json
from Lidl.items import *
import os
import scrapy
from scrapy.cmdline import execute
import pymysql
import Lidl.db_config as db

class LinkSpider(scrapy.Spider):
    name = 'link'
    allowed_domains = ['www.lidl.de']
    # start_urls = ['http://www.lidl.de/']

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
        cat_id_list = ['11','12','13','14','15','16','17','20','21']
        # cat_id_list = ['11']
        for cat_id in cat_id_list:
            url = f'https://www.lidl.de/q/api/category/spirituosen/h100066{cat_id}?offset=0&pageId=%2F10005566%2F10006611&fetchsize=600&locale=de_DE&assortment=DE&version=2.1.0&idsOnly=false&productsOnly=true&variant=default'
            cookies = {
                'kameleoonVisitorCode': 'ozjrup7nbnn3aa4l',
                'LidlID': '9d9e9fa0-a1a2-43a4-a5a6-a7a8a9aaabac',
                'OptanonAlertBoxClosed': '2023-12-26T08:31:27.267Z',
                'eupubconsent-v2': 'CP3YeWQP3YeWQAcABBENAgE4APLAAAAAAAYgF5wBQAKgAoABYAFsBeYAAABAkAIACoC8x0AIACoC8yUAEBeZSAEABUBeYAAA.flgAAAAAAAAA',
                'adSessionId': 'EA17756D-3437-44D6-BA4F-C7A42D52D2DB',
                'dtou': '0EB6794E0841CD0C2548A53D86FB9E1F',
                'mdLogger': 'false',
                'FPID': 'FPID2.2.%2F6MAcqJKqZjPCwC48v2bMNMBgoiJNMVGNJTUHpEi7Mw%3D.1703579489',
                'axd': '4348952402409561012',
                'et_uk': '864c8346227049db9ea90bdee2fa1daa',
                'tis': '',
                '_fbp': 'fb.1.1703579490005.413424542',
                'LidlID': 'ea29b785-11bc-432d-9994-3a4b9b3ec72e',
                'LidlIDu': 'true',
                'et_gk': 'e610a84a32e24260a54ec2d4df73f38d%7C29.04.2024%2010%3A59%3A39',
                '_clck': 'zwxidb%7C2%7Cfkl%7C0%7C1520',
                'UserVisits': 'current_visit_date:02.04.2024|last_visit_date:02.04.2024',
                '_gcl_au': '1.1.332329217.1712046223',
                '_ga_HTEPS28EL9': 'GS1.1.1712046222.7.1.1712046298.0.0.656984094',
                'kampyleUserSession': '1712046418908',
                'kampyleUserSessionsCount': '24',
                'kampyleUserPercentile': '0.022700339968806382',
                '_ga': 'GA1.2.1281552899.1703579489',
                '_uetvid': 'f0cc59e077b511eea8bd171b8c64dc53',
                'kampyleInvitePresented': 'true',
                'kampyleSessionPageCounter': '3',
                'kampylePageLoadedTimestamp': '1712046433475',
                '_ga_HTEPS28EL9': 'GS1.1.1712048739.8.0.1712048739.0.0.535862408',
                'cto_bundle': 'ohmzX19TMUlsaDZsY1ZtTTFEcjkxMVRwJTJGVUVtcm9FcUNSaGplZ0lzJTJGdkUyUnQ5QjExeTdlWVE1SEE1VWdzNmsxamtvSTZST0pORmVZdkNnU0x2cVhYb3RDRTBXM3Z3Nm52YTlHS0FqeFpaRUdjbEVYbTg0Slk0b1BYc1MyVTREbWhrbTYlMkJ6RGJobk1yamlOY1FVbGtIR2hzMkElM0QlM0Q',
                'DLPush': 'true',
                'inSession': 'true',
                'msAbTestVariationKey': 'ff01_flat-articles_a1',
                'OptanonConsent': 'isGpcEnabled=0&datestamp=Wed+May+01+2024+14%3A17%3A23+GMT%2B0530+(India+Standard+Time)&version=202403.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=3cc55945-a6d9-47ab-9c96-63fa2fc75cba&interactionCount=2&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CBG2559%3A1%2CC0042%3A1%2CBG2356%3A1%2CBG2357%3A1%2CBG2358%3A1%2CBG2359%3A1%2CBG2360%3A1%2CBG2361%3A1%2CBG2362%3A1%2CBG2363%3A1%2CC0048%3A1%2CC0049%3A1%2CC0045%3A1%2CC0046%3A1%2CC0047%3A1%2CC0055%3A1&AwaitingReconsent=false&geolocation=DE%3BHE',
            }

            headers = {
                'accept': 'application/mindshift.search+json;version=2',
                'accept-language': 'en-US,en;q=0.9',
                # 'cookie': 'kameleoonVisitorCode=ozjrup7nbnn3aa4l; LidlID=9d9e9fa0-a1a2-43a4-a5a6-a7a8a9aaabac; OptanonAlertBoxClosed=2023-12-26T08:31:27.267Z; eupubconsent-v2=CP3YeWQP3YeWQAcABBENAgE4APLAAAAAAAYgF5wBQAKgAoABYAFsBeYAAABAkAIACoC8x0AIACoC8yUAEBeZSAEABUBeYAAA.flgAAAAAAAAA; adSessionId=EA17756D-3437-44D6-BA4F-C7A42D52D2DB; dtou=0EB6794E0841CD0C2548A53D86FB9E1F; mdLogger=false; FPID=FPID2.2.%2F6MAcqJKqZjPCwC48v2bMNMBgoiJNMVGNJTUHpEi7Mw%3D.1703579489; axd=4348952402409561012; et_uk=864c8346227049db9ea90bdee2fa1daa; tis=; _fbp=fb.1.1703579490005.413424542; LidlID=ea29b785-11bc-432d-9994-3a4b9b3ec72e; LidlIDu=true; et_gk=e610a84a32e24260a54ec2d4df73f38d%7C29.04.2024%2010%3A59%3A39; _clck=zwxidb%7C2%7Cfkl%7C0%7C1520; UserVisits=current_visit_date:02.04.2024|last_visit_date:02.04.2024; _gcl_au=1.1.332329217.1712046223; _ga_HTEPS28EL9=GS1.1.1712046222.7.1.1712046298.0.0.656984094; kampyleUserSession=1712046418908; kampyleUserSessionsCount=24; kampyleUserPercentile=0.022700339968806382; _ga=GA1.2.1281552899.1703579489; _uetvid=f0cc59e077b511eea8bd171b8c64dc53; kampyleInvitePresented=true; kampyleSessionPageCounter=3; kampylePageLoadedTimestamp=1712046433475; _ga_HTEPS28EL9=GS1.1.1712048739.8.0.1712048739.0.0.535862408; cto_bundle=ohmzX19TMUlsaDZsY1ZtTTFEcjkxMVRwJTJGVUVtcm9FcUNSaGplZ0lzJTJGdkUyUnQ5QjExeTdlWVE1SEE1VWdzNmsxamtvSTZST0pORmVZdkNnU0x2cVhYb3RDRTBXM3Z3Nm52YTlHS0FqeFpaRUdjbEVYbTg0Slk0b1BYc1MyVTREbWhrbTYlMkJ6RGJobk1yamlOY1FVbGtIR2hzMkElM0QlM0Q; DLPush=true; inSession=true; msAbTestVariationKey=ff01_flat-articles_a1; OptanonConsent=isGpcEnabled=0&datestamp=Wed+May+01+2024+14%3A17%3A23+GMT%2B0530+(India+Standard+Time)&version=202403.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=3cc55945-a6d9-47ab-9c96-63fa2fc75cba&interactionCount=2&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CBG2559%3A1%2CC0042%3A1%2CBG2356%3A1%2CBG2357%3A1%2CBG2358%3A1%2CBG2359%3A1%2CBG2360%3A1%2CBG2361%3A1%2CBG2362%3A1%2CBG2363%3A1%2CC0048%3A1%2CC0049%3A1%2CC0045%3A1%2CC0046%3A1%2CC0047%3A1%2CC0055%3A1&AwaitingReconsent=false&geolocation=DE%3BHE',
                'priority': 'u=1, i',
                'referer': 'https://www.lidl.de/h/whisky-whiskey/h10006612?offset=24&pageId=10006418%2F10006612',
                'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            }
            page = 1
            pl_file_path = fr"{db.PAGESAVE}/Lidl_de_PL_{page}_{cat_id}.html"
            meta_dict = {"pl_file_path": pl_file_path, "count": 0, "offset": 0, "page": page,"cat_id": cat_id}
            if os.path.exists(pl_file_path):
                print('file:///' + pl_file_path)
                yield scrapy.Request(url='file:///' + pl_file_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
            else:
                yield scrapy.Request(url=url,headers=headers,callback=self.parse,cb_kwargs=meta_dict,dont_filter=True)

    def parse(self, response,**kwargs):
        item = LidlItem()
        cat_id = kwargs['cat_id']
        count = kwargs['count']
        offset = kwargs['offset']
        page = kwargs['page']
        pl_file_path = fr"{db.PAGESAVE}/Lidl_de_PL_{page}_{cat_id}.html"
        data =json.loads(response.text)
        product_urls= data['items']
        if product_urls:
            if not os.path.exists(pl_file_path):
                product_id_name = f"Lidl_de_PL_{page}_{cat_id}"
                db.pagesave(response, product_id_name)
            else:
                print("File Already Available...")

            for url in product_urls:
                print(url)
                count += 1
                product_url = url['url']
                if not product_url:
                    product_url = url['gridbox']['data']['canonicalUrl']
                if "https://www.lidl.de" not in product_url:
                    product_url = f"https://www.lidl.de{product_url}"
                print(product_url)
                Product_id = url['code']
                item['url'] =product_url
                item['Product_id'] = Product_id
                yield item

            total =data['numFound']
            if count < total:
                offset += 24
                page += 1
                print(offset)
                next_url = f'https://www.lidl.de/q/api/category/spirituosen/h100066{cat_id}?offset={offset}&pageId=%2F10005566%2F10006611&fetchsize=24&locale=de_DE&assortment=DE&version=2.1.0&idsOnly=false&productsOnly=true&variant=default'
                cookies = {
                    'LidlID': '7e7f8081-8283-4485-8687-88898a8b8c8d',
                    'inSession': 'true',
                    'OptanonAlertBoxClosed': '2023-09-11T12:45:38.543Z',
                    'eupubconsent-v2': 'CPx7G-QPx7G-QAcABBENDWCoAPLAAAAAAAYgF5wBwAKgAgABQACwAGQAWwF5gAAAHCQBAAKgAgABkAvMMABABQIAAgAoHQBAAKgAgABkAvMlABAXmUgBgAVABAAvMoABABkA.flgAAAAAAAAA',
                    'kameleoonVisitorCode': 'hi1uwnhtwadcx80q',
                    'adSessionId': 'BC6341B9-6C40-4B13-9B14-3EB3E13C3585',
                    '_gcl_au': '1.1.1192019713.1694436349',
                    '_gid': 'GA1.2.1437892594.1694436350',
                    'dt_sc': 'inenuo0me1upyhzyz5nnqhor%7C1694436350154',
                    'dtou': 'F19F95D347779F62101A89F79B9A4D6D',
                    '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22W8tUPyQone6tHji2Ixn6%22%7D',
                    '_fbp': 'fb.1.1694436352625.2034286894',
                    'mdLogger': 'false',
                    'kampyle_userid': 'bc3a-8ea9-9abc-937c-7c08-4fb4-741a-91f7',
                    'et_uk': '8be88c88670b4c049f0ab7ba0dda2422',
                    'et_gk': 'fa409f1ac52f491bbe58f367288b321f%7C16.10.2023%2006%3A32%3A32',
                    'FPID': 'FPID2.2.WOAPXqF61%2FjQFRkHgbdUVMOioAW95OSdUjtzdU%2B9wJs%3D.1694436350',
                    'SearchCollectorSession': 'E6ZOPuz',
                    'axd': '4339136155422799979',
                    'tis': '',
                    'FPLC': 'TyJ0OjIWa%2Bo0TVbGRO5pQJobiFKfJY2EG9KSzX%2FiL0FlqDrwt2466soLGEla4S8ApKPVrL2XaQ5tFIAKgokqmJd%2Ff7F5TJfDmYl1SCw4%2Fo%2Fu7kPhDFvVki5U1x6gfQ%3D%3D',
                    'lidl_locale': 'de-DE',
                    'DECLINED_DATE': '1694496499427',
                    'kampylePageLoadedTimestamp': '1694496535944',
                    'kampyleUserSession': '1694496586329',
                    'kampyleUserSessionsCount': '14',
                    'kampyleUserPercentile': '36.71307661509442',
                    'OptanonConsent': 'isGpcEnabled=0&datestamp=Tue+Sep+12+2023+11%3A05%3A31+GMT%2B0530+(India+Standard+Time)&version=202307.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=ad852544-c932-49b2-85a1-9be24da6ec02&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CBG2033%3A1%2CC0016%3A1%2CBG2034%3A1%2CBG2035%3A1%2CBG2036%3A1%2CBG2037%3A1%2CBG2038%3A1%2CBG2039%3A1%2CBG2040%3A1%2CBG2041%3A1%2CC0017%3A1%2CC0018%3A1%2CC0019%3A1%2CC0020%3A1%2CC0021%3A1&geolocation=AU%3BWA&AwaitingReconsent=false',
                    '_uetsid': '23ffc83050a111eea7f637647a08be0e',
                    '_uetvid': '2400131050a111ee9dbf7bd2b4e84523',
                    '_ga': 'GA1.2.193548788.1694436350',
                    'cto_bundle': 'V6MOiV9QRXpJTElEVFV2MGdWcmxxZnhFZHY0SkRJSXFqbUM0eEFzJTJCWW93V2tYWFJnOEJrcEclMkJQcEs4blJoT0JiUjhjWlR1ZGcyaGR5dVlLSFNnJTJCWlFpaFVWN25idjZkWEtHQmglMkJuNXVhT0tJRmFaeW1sZDY3SzZXc29wSmpwZTElMkZjWHU4cXdlaGlNUnQ1aENoeTVxRldGd0h3JTNEJTNE',
                    '_ga_HTEPS28EL9': 'GS1.1.1694495580.4.1.1694496934.0.0.0',
                    'kampyleSessionPageCounter': '7',
                }

                headers = {
                    'Referer': 'https://www.lidl.de/h/wein-spirituosen/h10005566?offset=48',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
                    'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                }
                meta_dict = {"count": count, "page": page,"offset":offset,"cat_id":cat_id}
                pl_file_path = fr"{db.PAGESAVE}/Lidl_de_PL_{page}_{cat_id}.html"
                if os.path.exists(pl_file_path):
                    print('file:///' + pl_file_path)
                    yield scrapy.Request(url='file:///' + pl_file_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
                else:
                    yield scrapy.Request(url=next_url,headers=headers,callback=self.parse,cb_kwargs=meta_dict)


if __name__ == '__main__':
    execute("scrapy crawl link ".split())