import json
import os
import re
from datetime import datetime
from scrapy.http import HtmlResponse
import pymysql
import requests
import scrapy
from scrapy.cmdline import execute
import Kaufland_de.db_config as db
from Kaufland_de.items import Kaufland_dataItem
from scrapy.http import Request
from Kaufland_de.EXPORT import file_generation

cookies = {
    'AB-optimizely_user': '58762a4d-1390-4792-b233-9b4107f12dbf',
    'AB-optimizely__device_type': 'desktop',
    'AB-optimizely__browser_name': 'Chrome',
    'AB-optimizely__environment': 'production',
    'OptanonAlertBoxClosed': '2023-11-08T18:30:54.761Z',
    'eupubconsent-v2': 'CP06RWQP06RWQAcABBENDeCoAPLAAAAAAAYgg1QDwAKgAgABQACwAGQC84JegmABMKCYgJjATIgmUCZcEzQTRgmmCa0E2ATeCDUAAAAA.flgAAAAAAAAA',
    '_fbp': 'fb.1.1699468256579.1065018895',
    '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22epdPQsnTnPoaCLtB6icC%22%7D',
    '_uetvid': 'f437c7107e6411ee946f53f9cb28e133',
    '_ga_9WNMNEZ2M0': 'deleted',
    '_gcl_au': '1.1.1451964774.1709532347',
    'FPAU': '1.1.1451964774.1709532347',
    'ALTSESSID': 'kh1pqdg4k07hocumcopsu8s8q7',
    'x-storefront': 'de',
    'api_ALTSESSID': 'kh1pqdg4k07hocumcopsu8s8q7',
    '_cs_c': '0',
    'hm_tracking': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NzNiZjg1ZmQ2Mjc2ZjYyMTc1N2Y0NWNlZTQyZmViZTkxMmI3N2Q0YzVmZDc0YzA5NTM0OTViMjg3ZjY5ZjJjZg%3D%3D.AfiLFdeMX%2F8uK4EGknvVIOAXcLTRwrYAiRMUYCzwcUc%3D',
    '_cs_mk': '0.5058039392550844_1716809619026',
    '_gid': 'GA1.2.1580709352.1716809620',
    'OptanonConsent': 'isGpcEnabled=0&datestamp=Mon+May+27+2024+17%3A21%3A59+GMT%2B0530+(India+Standard+Time)&version=202312.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=3d52be91-1a34-4cbf-b24d-3cc7e51877d1&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CC0030%3A1%2CC0041%3A1%2CC0053%3A0%2CC0049%3A0%2CC0054%3A0%2CC0047%3A0%2CC0055%3A0%2CBG2530%3A1%2CBG2520%3A1%2CBG2487%3A1%2CBG2488%3A1%2CBG2489%3A1%2CBG2490%3A1%2CBG2491%3A1%2CBG2492%3A1%2CBG2493%3A1&geolocation=IN%3BGJ&AwaitingReconsent=false',
    '_ga': 'GA1.2.2137258739.1699468255',
    '_uetsid': 'f6da7ca01c1c11efb621c7656c2fd38b',
    '_dc_gtm_UA-27218006-5': '1',
    '_gat_UA-27218006-5': '1',
    'api_hm_lsi': '352995286%2C331146110',
    '_cs_id': '486ea57c-7c01-a0ae-940f-5bb89d39569d.1715226100.2.1716810722.1716809620.1713531742.1749390100889.1',
    '_cs_s': '5.0.0.1716812522319',
    '_ga_9WNMNEZ2M0': 'GS1.1.1716809619.16.1.1716810722.0.0.576871489',
    'SQsession': '1716810726183b705q',
    '_dd_s': 'rum=0&expire=1716811643726&logs=0',
}

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    # 'Cookie': 'AB-optimizely_user=58762a4d-1390-4792-b233-9b4107f12dbf; AB-optimizely__device_type=desktop; AB-optimizely__browser_name=Chrome; AB-optimizely__environment=production; OptanonAlertBoxClosed=2023-11-08T18:30:54.761Z; eupubconsent-v2=CP06RWQP06RWQAcABBENDeCoAPLAAAAAAAYgg1QDwAKgAgABQACwAGQC84JegmABMKCYgJjATIgmUCZcEzQTRgmmCa0E2ATeCDUAAAAA.flgAAAAAAAAA; _fbp=fb.1.1699468256579.1065018895; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22epdPQsnTnPoaCLtB6icC%22%7D; _uetvid=f437c7107e6411ee946f53f9cb28e133; _ga_9WNMNEZ2M0=deleted; _gcl_au=1.1.1451964774.1709532347; FPAU=1.1.1451964774.1709532347; ALTSESSID=kh1pqdg4k07hocumcopsu8s8q7; x-storefront=de; api_ALTSESSID=kh1pqdg4k07hocumcopsu8s8q7; _cs_c=0; hm_tracking=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NzNiZjg1ZmQ2Mjc2ZjYyMTc1N2Y0NWNlZTQyZmViZTkxMmI3N2Q0YzVmZDc0YzA5NTM0OTViMjg3ZjY5ZjJjZg%3D%3D.AfiLFdeMX%2F8uK4EGknvVIOAXcLTRwrYAiRMUYCzwcUc%3D; _cs_mk=0.5058039392550844_1716809619026; _gid=GA1.2.1580709352.1716809620; OptanonConsent=isGpcEnabled=0&datestamp=Mon+May+27+2024+17%3A21%3A59+GMT%2B0530+(India+Standard+Time)&version=202312.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=3d52be91-1a34-4cbf-b24d-3cc7e51877d1&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CC0030%3A1%2CC0041%3A1%2CC0053%3A0%2CC0049%3A0%2CC0054%3A0%2CC0047%3A0%2CC0055%3A0%2CBG2530%3A1%2CBG2520%3A1%2CBG2487%3A1%2CBG2488%3A1%2CBG2489%3A1%2CBG2490%3A1%2CBG2491%3A1%2CBG2492%3A1%2CBG2493%3A1&geolocation=IN%3BGJ&AwaitingReconsent=false; _ga=GA1.2.2137258739.1699468255; _uetsid=f6da7ca01c1c11efb621c7656c2fd38b; _dc_gtm_UA-27218006-5=1; _gat_UA-27218006-5=1; api_hm_lsi=352995286%2C331146110; _cs_id=486ea57c-7c01-a0ae-940f-5bb89d39569d.1715226100.2.1716810722.1716809620.1713531742.1749390100889.1; _cs_s=5.0.0.1716812522319; _ga_9WNMNEZ2M0=GS1.1.1716809619.16.1.1716810722.0.0.576871489; SQsession=1716810726183b705q; _dd_s=rum=0&expire=1716811643726&logs=0',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}
class DataSpider(scrapy.Spider):
    name = 'data_extractor'
    allowed_domains = ['www.kaufland.de']
    handle_httpstatus_list = [404,500]

    # start_urls = ['http://www.kaufland.de/']
    # PAGESAVE = 'D:\\Page_save\\Kaufland\\'

    def __init__(self,start=0,end=0):
        # todo this is for database Connection and feching links and pagesave
        self.start = start
        self.end = end
        self.conn = pymysql.Connect(
            host=db.db_host,
            user=db.db_user,
            password=db.db_password,
            database=db.db_name
        )
        self.db_table = f"{db.db_links_table}"
        self.cur = self.conn.cursor()
        selec_q = f'select * from {self.db_table} where status="pending" and id between {self.start} and {self.end}'
        self.cur.execute(selec_q)
        self.all_d = self.cur.fetchall()
        self.PAGESAVE = db.PAGESAVE
        if not os.path.exists(self.PAGESAVE):
            os.makedirs(self.PAGESAVE)
        # self.proxy = f"http://scraperapi:de51e4aafe704395654a32ba0a14494d:@proxy-server.scraperapi.com:8001"
        self.proxy = 'http://9dbe950ef6284a5da9e7749db9f7cbd1:@api.zyte.com:8011'


    def start_requests(self):
        for i in self.all_d:
            URL = i[1]
            Platform_URL=URL.split('?')[0]

            Product_id = i[2]
            Type=i[4]

            # "https://quotes.toscrape.com/"
            # response = requests.get(Platform_URL, cookies=cookies, headers=headers)
            # response_text=response.text
            pdp_path = f"{db.PAGESAVE}/{Product_id}.html"
            meta_dict = {
                "Platform_URL": Platform_URL,
                "Product_id": Product_id,
                "Type": Type,
                "pdp_path": pdp_path
            }
            if os.path.exists(pdp_path):
                print('file:///' + pdp_path)
                yield scrapy.Request(url='file:///' + pdp_path, cb_kwargs=meta_dict, callback=self.parse, dont_filter=True)
            else:
                yield Request(url=Platform_URL,headers=headers,cookies=cookies,dont_filter=True,callback=self.parse,cb_kwargs=meta_dict,meta={'proxy': self.proxy})
            # break

    def parse(self,response,**kwargs):
        item = Kaufland_dataItem()
        item['Platform_URL'] = kwargs['Platform_URL']
        item['Type'] = kwargs['Type']
        Product_id =kwargs['Product_id']
        pdp_path =kwargs['pdp_path']
        # response = HtmlResponse('hss', body=response_text, encoding='utf8')
        # todo this is for update status on link table
        if response.status == 404:
            print(f"""UPDATE {db.db_links_table} SET STATUS = '404' where product_id='{Product_id}'""")
            self.cur.execute(f"""UPDATE {db.db_links_table} SET STATUS = '404' where product_id='{Product_id}'""")
            self.conn.commit()
            return None

        item['Product_id'] = Product_id

        if not os.path.exists(pdp_path):
            # todo pagesave
            db.pagesave(response, Product_id )
        else:
            print("File Already Available...")

        # todo this is for sku_name, category
        SKU_Name = response.xpath('//h1[contains(@class,"rd-title")]//text()').get()
        SKU_Name = re.sub(' +', ' ', SKU_Name).strip()
        item['SKU_Name'] = SKU_Name

        try:

            Category1 = response.xpath('//div[@class="rd-breadcrumb__item"]//a//text()').getall()[-1]
            Category = Category1.strip()
        except Exception as e:
            Category=''
            print(e)
        item['Category'] = Category

        if not item['Category']:
            try:
                Category1 = response.xpath('//meta[@data-hid="description"]/@content').get().split()[0]
                Category = Category1.strip()
            except Exception as e:
                # catgory_name=re.findall(r'></path></svg></span>\n (.*)</span> <span')
                Category = ''
                print(e)
        item['Category'] = Category

        # todo this is for image_url
        img_list = []
        aa = response.xpath("//div[@class='swiper-wrapper rd-gallery__wrapper ']//img")
        if aa:
            for i in aa:
                img_slug = i.xpath('./@src').get()
                img_link =img_slug.split('100/')[1]
                img = 'https://media.cdn.kaufland.de/product-images/1024x1024/'+img_link
                img_list.append(img)
            img1 = set(img_list)
            img2 = list(img1)
            img_url = ' | '.join(img2)
            item['Image_Urls'] = img_url
        else:
            aa= response.xpath('//div[@class="swiper-wrapper rd-gallery__wrapper"]//img/@src').get()
            item['Image_Urls'] = aa

        # todo this is for price_in_local
        item['Price_In_Local']='NA'
        item['Price_In_Local_USD']= 'NA'
        price =  response.xpath('//span[@class="rd-price-information__price"]//text()').get('').strip()
        price1 = price.split('/')[0].replace('(', '').replace('\xa0€', '')
        if '.' in price1 and ',' in price1:
            price2 = price1.replace('.', '').replace(',', '.')
            if price2:
                try:
                    price3 = float(price2)
                    price4 = "%.2f" % price3
                    item['Price_In_Local'] = price4

                    Price_per_unit_USD1 = float(price4) * 1.1500
                    Price_per_unit_USD = "%.2f" % Price_per_unit_USD1
                    item['Price_In_Local_USD']= Price_per_unit_USD
                except:
                    item['Price_In_Local_USD'] = ''
        else:
            price2 = price1.replace(',', '.')
            if price2:
                try:
                    price3 = float(price2)

                    price4 = "%.2f" % price3
                    item['Price_In_Local'] = price4

                    Price_per_unit_USD1 = float(price4) * 1.1500
                    Price_per_unit_USD = "%.2f" % Price_per_unit_USD1
                    item['Price_In_Local_USD'] = Price_per_unit_USD
                except:
                    item['Price_In_Local_USD'] = ''


        # todo this is for price_range
        item['Price_Range'] = 'NA'
        if item['Price_In_Local_USD'] != 'NA':
            price_in_usd = float(item['Price_In_Local_USD'])
            if price_in_usd < 20: item['Price_Range'] = '<20'
            if price_in_usd >= 20 and price_in_usd <= 30: item['Price_Range'] = '20-30'
            if price_in_usd >= 30 and price_in_usd <= 40: item['Price_Range'] = '30-40'
            if price_in_usd >= 40 and price_in_usd <= 50: item['Price_Range'] = '40-50'
            if price_in_usd >= 50 and price_in_usd <= 60: item['Price_Range'] = '50-60'
            if price_in_usd >= 60 and price_in_usd <= 90: item['Price_Range'] = '60-90'
            if price_in_usd >= 90 and price_in_usd <= 150: item['Price_Range'] = '90-150'
            if price_in_usd > 150: item['Price_Range'] = '150+'

        # todo this is for Price_per_unit_Local
        item['Price_per_unit_USD']=''
        item['Price_per_unit_Local']=''

        price = response.xpath('//p[@class="rd-buybox-comparison__base-price"]/span//text()').get('').strip()
        price1 = price.split('/')[0].replace('(', '').replace('\xa0€', '')
        if '.' in price1 and ',' in price1:
            price2 = price1.replace('.', '').replace(',', '.')
            if price2:
                try:
                    price3 = float(price2)
                    price4 = "%.2f" % price3
                    item['Price_per_unit_Local'] = price4
                    Price_per_unit_USD1 = float(price4) * 1.1500
                    Price_per_unit_USD = "%.2f" % Price_per_unit_USD1
                    item['Price_per_unit_USD'] = Price_per_unit_USD
                except:
                    item['Price_per_unit_USD'] = ''
        else:
            price2 = price1.replace(',', '.')
            if price2:
                try:
                    price3 = float(price2)

                    price4 = "%.2f" % price3
                    item['Price_per_unit_Local'] = price4

                    Price_per_unit_USD1 = float(price4) * 1.1500
                    Price_per_unit_USD = "%.2f" % Price_per_unit_USD1
                    item['Price_per_unit_USD'] = Price_per_unit_USD
                except:
                    item['Price_per_unit_USD'] =''

        if item['Price_per_unit_USD']:
            item['Price_per_unit_USD_New']=item['Price_per_unit_USD']
        else:
            item['Price_per_unit_USD_New']=''

        # todo this is for Manufacturer,Pack_Size_Local,Pack_Size,ABV,Origin
        # desc = response.xpath('''//script[contains(text(),"window.__PDPFRONTEND__=")]/text()''').get()
        desc = response.xpath('''//div[contains(@class,"rd-description-teaser__description-text")]/text()''').get('')
        b = re.findall(r'{name(.*?)}]', desc)
        for i in b:
            if 'Hersteller' in i:
                manufact1 = re.findall(r'text:"(.*?)",link', i)
                manufact = ''.join(manufact1)
                item['Manufacturer'] = manufact

            if 'Inhalt' in i or 'content_volume' in i:
                pack_size1 = re.findall(r'text:"(.*?)",link', i)
                if not pack_size1:
                    pack_size1 = re.findall(r'=(.*?)\\', i)
                pack_size_local = ''.join(pack_size1).replace(',','.')
                item['Pack_Size_Local'] = ''.join(pack_size_local)
                print(pack_size_local)

                Pack_Sizes = pack_size_local
                numeric_value = re.findall('([-+]?(?:\d*\.*\d+))', Pack_Sizes, flags=re.IGNORECASE)
                if numeric_value:
                    numeric_value = float(numeric_value[0])
                unit = Pack_Sizes[-5:].strip()

                if unit.lower() == 'cl':
                    Pack_Size = numeric_value * 10
                elif unit.lower() == 'litre' or ('l' in unit.lower() and 'ml' not in unit.lower()):
                    Pack_Size = numeric_value * 1000

                else:
                    Pack_Size = numeric_value

                item['Pack_Size'] = Pack_Size

            if 'Alkoholgehalt' in i:
                ABV1 = re.findall(r'text:"(.*?)",link', i)
                ABV = ''.join(ABV1).split('%')[0]
                item['ABV'] = ABV.replace(',','.')
                print(ABV)

            if 'Herkunft' in i:
                origin = re.findall(r'text:"(.*?)",link', i)
                item['Origin'] =''.join(origin)
                print(origin)

        # todo this is for Pack_Size_Local, Pack_Size
        item['Pack_Size_Local'] = 'NA'
        aa = re.findall(r'([-+]?(?:\d*[\.|\,]*\d+))\s*X\s*([-+]?(?:\d*[\.|\,]*\d+))\s*X\s*([-+]?(?:\d*[\.|\,]*\d+))\s*[-]?\s*([a-z]+)',item['SKU_Name'].replace(',', '.'), flags=re.IGNORECASE)
        if aa:
            for i in aa:
                pack = str(float(i[0]) * float(i[1])* float(i[2]))
                pack = f"{pack}{i[3]}"

                pack_size = db.pack_size(self, pack)
                if pack_size:
                    item['Pack_Size_Local'] = f'{i[0]} x {i[1]} x {i[2]} {i[3]}'
                    item['Pack_Size'] = pack_size[0]

        elif re.findall(r'([-+]?(?:\d*[\.|\,]*\d+))\s*X\s*([-+]?(?:\d*[\.|\,]*\d+))\s*[-]?\s*([a-z]+)',item['SKU_Name'].replace(',', '.'), flags=re.IGNORECASE):
            aa = re.findall(r'([-+]?(?:\d*[\.|\,]*\d+))\s*X\s*([-+]?(?:\d*[\.|\,]*\d+))\s*[-]?\s*([a-z]+)',item['SKU_Name'].replace(',', '.'), flags=re.IGNORECASE)
            if aa:
                for i in aa:
                    pack = str(float(i[0].replace('|','')) * float(i[1]))
                    pack = f"{pack}{i[2]}"
                    pack_size = db.pack_size(self,pack)
                    if pack_size:
                        item['Pack_Size_Local'] = f'{i[0]} x {i[1]} {i[2]}'
                        item['Pack_Size'] = pack_size[0]

        else:
            string = SKU_Name
            pack = db.pack_size(self,string.replace(',','.').replace('+',''))
            if pack:
                if pack[1].startswith('.'):
                    item['Pack_Size_Local'] = pack[1].replace('.', '')
                    item['Pack_Size'] = db.pack_size( self,item['Pack_Size_Local'])[0]

                else:
                    item['Pack_Size_Local'] = pack[1]
                    item['Pack_Size'] = pack[0]

        # todo this is for Pack_Size_New
        a = ['NA', '704 ml', '1230 ml', '250 ml', '14.8 cl', '1600 ml', '2150 ml', '2100 ml', '1450 ml', '1980 ml',
             '200 ml', '650 ml', '950 ml', '2.25 l', '2250 ml', '1780 ml', '150 ml', '1580 ml', '50 ml', '1.125 l',
             '1300 ml', '1050 ml', '550 ml', '3500 ml', '1120 ml', '1280 ml', '2450 ml', '701 ml', '70 cl', '900 ml',
             '846 ml', '2500 ml', '1350 ml', '2750 ml', '1.75 l', '2200 ml', '2800 ml', '1250 ml', '800 ml', '1775 ml',
             '3000 ml', '10 cl', '3000 l', '5 cl', '330 ml', '440 ml', '568 ml', '125 ml', '25 cl', '275 ml', '20 cl',
             '180 ml', '33 cl', '250ml', '125ml', '200ml', '100ml', '330ml', '720ml', '20cl', '2640 ml', '5cl', '4x250',
             '10x250', '4x5cl', '4.5 l', '1750ml', '375ml', '5250ml', '640ml', '50ml', '300ml', '725ml', '1125ml',
             '275ml', '1136ml', '4500ml', '30 ml', '355ml', '745ml', '400ml', '3000ml', '100 ml', '6 x 375ml', '40mL',
             '1450ML', '360mL', '6 x 30mL', '180mL', '650mL', '230mL', '6x50mL', '370mL', '8x50mL', '10330ML',
             '10 x 375mL', '175mL', '3 x 200mL', '12x40mL', '110mL', '475mL', '2330mL', '24375ML', '130mL', '1420ML',
             '187mL', '220mL', '4x50mL', '1.125L', '900mL', '20mL', '1.75L', '95mL', '1800mL', '3.4L', '800ML', '30mL',
             '3L', '60mL', '80mL', '3500mL', '2.1L', '4x 200mL', '550mL', '3x50ml', '1.125 Litre', '440ml', '270ml',
             '660ml', '1lt', '1l', '510ml', '320ml', '120ml', '140ml', '345ml', '300 l', '225 l', '72 cl', '14.6 cl',
             '12 cl', '2 cl', '375 ml', '3.5 cl', '4 cl', '1.75 cl', '500 l', '215 l', '130 l', '1 cl', '47.3 cl',
             '255 l', '115 l', '290 l', '400 l', '125 l', '7.5 cl', '6 cl', '1707 l', '210 l', '44 cl', '37.5 cl',
             '67 cl', '30 cl', '71ml', '0.200l', '660 ml', '300 ml', '7.1 cl', '620 ml', '355 ml', '1100 ml', '1320 ml',
             '510 ml', '201 ml', '1.0 cl', '3 l', '300 cl', '75 l', '0.2 l', '100 cl', '50.00 ml', '180.00 ml',
             '720.00 ml', '2100.00 ml', '4000.00 ml', '2760.00 ml', '300.00 ml', '1800.00 ml', '100.00 ml', '360.00 ml',
             '200.00 ml', '660.00 ml', '75.00 ml', '475.00 ml', '248.00 ml', '1.8L', '2200ML', '4L', '1920ML', '1440ML',
             '8 cl', '23 cl', '1738 cl', '40 ml', '770 ml', '20 ml', '275ml x 6', '275ml x 24', '1.75 Litros',
             '175 Centilitros', '60 Centilitros', '17.5 Centilitros', '35.50 cl', '33 Centilitros', '3 Litros',
             '20 Centilitros', '682.8 g', '187 g', '250 Mililitros', '25 Centilitros']
        if item['Pack_Size_Local'] in a:
            item['Pack_Size_New'] = 'Others'

        # todo this is for Age_of_whisky
        item['Age_of_Whiskey'] = ''
        age_whisky = re.findall(r'([0-9]*\.?[0-9])\s*year', item['SKU_Name'], flags=re.IGNORECASE)
        if not age_whisky:
            age_whisky = re.findall(r'([0-9]*\.?[0-9])\s*Years', item['SKU_Name'], flags=re.IGNORECASE)
        if not age_whisky:
            age_whisky = re.findall(r'([0-9]*\.?[0-9])\s*-year', item['SKU_Name'], flags=re.IGNORECASE)
        if not age_whisky:
            age_whisky = re.findall(r'([0-9]*\.?[0-9])\s*yo', item['SKU_Name'], flags=re.IGNORECASE)
        if not age_whisky:
            age_whisky = re.findall(r'([0-9]*\.?[0-9])\s*yr', item['SKU_Name'], flags=re.IGNORECASE)
        if not age_whisky:
            age_whisky = re.findall(r'([0-9]*\.?[0-9])\s*Jahre', item['SKU_Name'], flags=re.IGNORECASE)
        if age_whisky:
            item['Age_of_Whiskey'] = age_whisky[0]



        current_date = datetime.now()
        scrape_date = current_date.strftime("%d_%m_%Y")
        item['scrape_date'] = scrape_date
        item['Platform_Name'] = "Kaufland.de"
        item['Major_Region'] = "Europe"
        item['Country'] = "Germany"
        item['Market_Clusters'] = "EMEA Develop"
        item['Sub_Category'] = "NA"
        item['Sector'] = "NA"
        item['Sub_Sector'] = "NA"
        item['Brand'] = "NA"
        item['Sub_Brand'] = "NA"
        item['Type_of_Promo'] = "NA"
        item['Promo_Price_Local'] = "NA"
        item['Promo_Price_USD'] = "NA"
        item['Distillery'] = "NA"
        item['Pack_type'] = "NA"
        item['Tasting_Notes'] = "NA"
        item['Standard_Currency'] = "USD"

        yield item
        # print(item)

    def closed(self, reason):
        """This method is called when the spider finishes."""
        print("Spider closed, reason:", reason)
        file_generation()  # Call your file generation function here


if __name__ == '__main__':
       execute("scrapy crawl data_extractor -a start=1 -a end=100000000000".split())
       # execute("scrapy crawl data -a start=1 -a end=1".split())


