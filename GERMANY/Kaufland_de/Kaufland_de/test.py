import json
import os
import re
from datetime import datetime

import pymysql
import scrapy
from scrapy.cmdline import execute
import Kaufland_de.db_config as db
from Kaufland_de.items import Kaufland_dataItem

cookies = {
    'x-storefront': 'de',
    'ALTSESSID': 'v82upf4ile88kn9plugjnfbi8p',
    'AB-optimizely__device_type': 'desktop',
    'AB-optimizely__browser_name': 'Chrome',
    'AB-optimizely__environment': 'production',
    'OptanonAlertBoxClosed': '2023-12-26T08:14:10.257Z',
    'eupubconsent-v2': 'CP3YeWQP3YeWQAcABBENAgE4APLAAAAAAAYgJnQBwAKgAoABYAvMCX4EygJnAAAAECQAgAKgLzHQAgAKgLzJQAQF5lIAQAFQF5gA.flgAAAAAAAAA',
    '_gcl_au': '1.1.792089184.1703578450',
    'FPAU': '1.1.792089184.1703578450',
    '_cs_c': '0',
    '_fbp': 'fb.1.1703578452122.872523337',
    'axd': '4348952402409561012',
    'tis': '',
    'api_ALTSESSID': 'v82upf4ile88kn9plugjnfbi8p',
    '_cs_id': '6e282815-d9fd-a9ef-8f8d-871fa8db1af4.1703578451.2.1703592381.1703591325.1.1737742451453',
    'AB-optimizely_user': '6a7882e9-9624-45ef-8abe-c0300ba344c7',
    'hm_tracking': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NDM2N2U0YjIyZTdhMmIzZDk2YzA1NzMxZWMxZTJjODU5ZWRjZTIzMTBjZjU5N2I1ZDJjNmFiNTIwYjQwM2M1Mw%3D%3D.KLiVDBrkZ9jByYwfJ%2B5%2BXjDrDyKdTMJkrfwBxbb38lo%3D',
    '_gid': 'GA1.2.1777184767.1706535378',
    'OptanonConsent': 'isGpcEnabled=0&datestamp=Mon+Jan+29+2024+20%3A08%3A43+GMT%2B0530+(India+Standard+Time)&version=202312.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=1d93b29e-69df-4ab2-8793-6b6d3a0ce9e1&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CBG2530%3A1%2CC0030%3A1%2CBG2520%3A1%2CBG2487%3A1%2CBG2488%3A1%2CBG2489%3A1%2CBG2490%3A1%2CBG2491%3A1%2CBG2492%3A1%2CBG2493%3A1%2CC0053%3A1%2CC0049%3A1%2CC0054%3A1%2CC0041%3A1%2CC0047%3A1%2CC0055%3A1&geolocation=DE%3BHE&AwaitingReconsent=false',
    'hm_lsi': '420994782%2C436579023%2C456955546',
    'api_hm_lsi': '420994782%2C436579023%2C456955546',
    '_cs_mk': '0.033163889237375344_1706539124447',
    '_ga_9WNMNEZ2M0': 'GS1.1.1706539125.5.0.1706539125.0.0.0',
    '_uetsid': '61452c10beab11ee87e6171c237ab670',
    '_uetvid': '4434aae077ae11eeb6fc838426f35e84',
    '_dc_gtm_UA-27218006-5': '1',
    '_ga': 'GA1.2.1461653445.1703578451',
    '_gat_UA-27218006-5': '1',
    '_dd_s': 'logs=0&expire=1706540030405&rum=0',
}

headers = {
    'authority': 'www.kaufland.de',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    # 'cookie': 'x-storefront=de; ALTSESSID=v82upf4ile88kn9plugjnfbi8p; AB-optimizely__device_type=desktop; AB-optimizely__browser_name=Chrome; AB-optimizely__environment=production; OptanonAlertBoxClosed=2023-12-26T08:14:10.257Z; eupubconsent-v2=CP3YeWQP3YeWQAcABBENAgE4APLAAAAAAAYgJnQBwAKgAoABYAvMCX4EygJnAAAAECQAgAKgLzHQAgAKgLzJQAQF5lIAQAFQF5gA.flgAAAAAAAAA; _gcl_au=1.1.792089184.1703578450; FPAU=1.1.792089184.1703578450; _cs_c=0; _fbp=fb.1.1703578452122.872523337; axd=4348952402409561012; tis=; api_ALTSESSID=v82upf4ile88kn9plugjnfbi8p; _cs_id=6e282815-d9fd-a9ef-8f8d-871fa8db1af4.1703578451.2.1703592381.1703591325.1.1737742451453; AB-optimizely_user=6a7882e9-9624-45ef-8abe-c0300ba344c7; hm_tracking=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.NDM2N2U0YjIyZTdhMmIzZDk2YzA1NzMxZWMxZTJjODU5ZWRjZTIzMTBjZjU5N2I1ZDJjNmFiNTIwYjQwM2M1Mw%3D%3D.KLiVDBrkZ9jByYwfJ%2B5%2BXjDrDyKdTMJkrfwBxbb38lo%3D; _gid=GA1.2.1777184767.1706535378; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Jan+29+2024+20%3A08%3A43+GMT%2B0530+(India+Standard+Time)&version=202312.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=1d93b29e-69df-4ab2-8793-6b6d3a0ce9e1&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CBG2530%3A1%2CC0030%3A1%2CBG2520%3A1%2CBG2487%3A1%2CBG2488%3A1%2CBG2489%3A1%2CBG2490%3A1%2CBG2491%3A1%2CBG2492%3A1%2CBG2493%3A1%2CC0053%3A1%2CC0049%3A1%2CC0054%3A1%2CC0041%3A1%2CC0047%3A1%2CC0055%3A1&geolocation=DE%3BHE&AwaitingReconsent=false; hm_lsi=420994782%2C436579023%2C456955546; api_hm_lsi=420994782%2C436579023%2C456955546; _cs_mk=0.033163889237375344_1706539124447; _ga_9WNMNEZ2M0=GS1.1.1706539125.5.0.1706539125.0.0.0; _uetsid=61452c10beab11ee87e6171c237ab670; _uetvid=4434aae077ae11eeb6fc838426f35e84; _dc_gtm_UA-27218006-5=1; _ga=GA1.2.1461653445.1703578451; _gat_UA-27218006-5=1; _dd_s=logs=0&expire=1706540030405&rum=0',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}
class DataSpider(scrapy.Spider):
    name = 'data'
    allowed_domains = ['www.kaufland.de']
    handle_httpstatus_list = [404]

    # start_urls = ['http://www.kaufland.de/']
    # PAGESAVE = 'D:\\Page_save\\Kaufland\\'

    def __init__(self,start=0,end=0):
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


    def start_requests(self):
        for i in self.all_d:
            URL = i[1]
            Platform_URL=URL.split('?')[0]

            Product_id = i[2]


            yield scrapy.Request(url=Platform_URL,
                                 headers=headers,
                                 # cookies=cookies,
                                 dont_filter=True,
                                 callback=self.parse,
                                 meta={'Platform_URL':Platform_URL,
                                       'Product_id':Product_id})


    def parse(self, response):

        item = Kaufland_dataItem()
        item['Platform_URL'] = response.meta['Platform_URL']


        Product_id = response.meta['Product_id']
        if response.status == 404:
            self.cur.execute(
                f"""UPDATE {db.db_links_table} SET STATUS = '404' where url='{item["Platform_URL"]}'""")
            self.conn.commit()
            return None
        item['Product_id'] = Product_id

        db.pagesave(response, Product_id)

        # open(self.PAGESAVE + str(Product_id) + ".html", "wb").write(response.body)


        SKU_Name = response.xpath('//h1[@class="rd-title"]//text()').get()
        SKU_Name = re.sub(' +', ' ', SKU_Name).strip()
        item['SKU_Name'] = SKU_Name

        Category1 = response.xpath('//div[@class="rd-breadcrumb__item"]//a//text()').getall()[-1]
        Category = Category1.strip()
        item['Category'] = Category

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

                    Price_per_unit_USD1 = float(price4) * 1.0500
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

                    Price_per_unit_USD1 = float(price4) * 1.0500
                    Price_per_unit_USD = "%.2f" % Price_per_unit_USD1
                    item['Price_In_Local_USD'] = Price_per_unit_USD
                except:
                    item['Price_In_Local_USD'] = ''

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

        price = response.xpath('//p[@class="rd-buybox-comparison__base-price"]/span//text()').get('').strip()
        price1 = price.split('/')[0].replace('(', '').replace('\xa0€', '')
        if '.' in price1 and ',' in price1:
            price2 = price1.replace('.', '').replace(',', '.')
            if price2:
                try:
                    price3 = float(price2)
                    price4 = "%.2f" % price3
                    item['Price_per_unit_Local'] = price4
                    Price_per_unit_USD1 = float(price4) * 1.0500
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

                    Price_per_unit_USD1 = float(price4) * 1.0500
                    Price_per_unit_USD = "%.2f" % Price_per_unit_USD1
                    item['Price_per_unit_USD'] = Price_per_unit_USD
                except:
                    item['Price_per_unit_USD'] =''


        desc = response.xpath('''//script[contains(text(),"window.__PDPFRONTEND__=")]/text()''').get()
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
        item['Platform_Name'] = "Kaufland"
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


# if __name__ == '__main__':
#        execute("scrapy crawl data -a start=1 -a end=10000000000000000".split())
#

