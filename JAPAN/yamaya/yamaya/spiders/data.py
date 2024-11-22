import os
import re
from datetime import datetime
from num2words import num2words
import pymysql
import scrapy
from scrapy.cmdline import execute
from yamaya.items import *
import yamaya.db_config as db
# from googletrans import Translator


class DataSpider(scrapy.Spider):
    name = "data"
    allowed_domains = ["drive.yamaya.jp"]

    def __init__(self, start=0, end=0, name=None, **kwargs):
        super().__init__(name, **kwargs)
        self.start = start
        self.end = end
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password)
        self.cursor = self.con.cursor(pymysql.cursors.DictCursor)
        self.cursor.execute(f"USE {db.db_name};")

    def start_requests(self):
        self.cursor.execute(
            f"SELECT * FROM {db.db_links_table} WHERE status = 'pending' and id between {self.start} and {self.end}""")
        # f"SELECT * FROM {db.db_links_table} WHERE status = 'pending' limit {self.start}, {self.end}""")
        links = self.cursor.fetchall()
        for link in links:
            url = link['url']
            Type=link['Type']
            pid = link['product_id']
            meta = {'id': link['Id'],
                    'url': url,
                    'Type':Type,
                    'product_id': pid}

            filepath = f'{db.PAGESAVE}{pid}.html'
            if os.path.exists(filepath):
                yield scrapy.Request(url=f'file:///{filepath}',
                                     meta=meta,
                                     )
            else:
                yield scrapy.Request(url=url, meta=meta)

    def parse(self, response):
        item = YamayaItem()
        exchange_rate = 0.0068
        item['Id'] = response.meta['id']
        item['Type'] = response.meta['Type']
        item['Platform_Name'] = 'Yamaya'
        item['Standard_Currency'] = 'USD'
        item['Major_Region'] = 'Asia Pacific'
        item['Country'] = 'Japan'
        item['Market_Clusters'] = 'APAC Develop'
        item['scrape_date'] = datetime.today().strftime('%d_%m_%Y')
        item['Platform_URL'] = response.meta['url']
        item['Product_id'] = product_id = response.meta['product_id']

        # PAGESAVE
        filepath = f"{db.PAGESAVE}{item['Product_id']}.html"
        if not os.path.exists(filepath):
            db.pagesave(path=db.PAGESAVE, response=response, product_id=item['Product_id'])

        # SKU NAME
        item['SKU_Name'] = re.sub(r'\s+', ' ', response.xpath('//div[@class="card"]//h4/text()').get())

        # CATEGORY
        item['Category'] = re.sub(r'\s+', ' ', ' '.join(response.xpath(
            '//table[@class="table table-sm table-borderless"]//th[text()="分類"]/following-sibling::td//text()').getall())).strip()

        # ORIGIN AND COUNTRY OF ORIGIN
        item['Origin'] = item['Country_of_Origin'] = response.xpath(
            '//table[@class="table table-sm table-borderless"]//th[text()="原産"]/following-sibling::td//text()').get()

        # ABV
        item['ABV'] = response.xpath(
            '//table[@class="table table-sm table-borderless"]//th[text()="アルコール度数"]/following-sibling::td//text()').get(
            '').replace(' 度', '')

        # IMAGES
        item['Image_Urls'] = 'https://drive.yamaya.jp' + response.xpath(
            '//div[@class="card"]//img/@src').get() if response.xpath('//div[@class="card"]//img/@src').get() else 'NA'

        # PACK SIZE AND PACK SIZE LOCAL
        item['Pack_Size_Local'] = 'NA'
        item['Pack_Size'] = 'NA'
        pack_size = db.pack_size(value=item['SKU_Name'])
        if pack_size:
            item['Pack_Size_Local'] = pack_size[1]
            item['Pack_Size'] = pack_size[0]
        if item['Pack_Size'] == 'NA':
            pack_size = response.xpath(
                '//table[@class="table table-sm table-borderless"]//th[text()="容量"]/following-sibling::td//text()').get()
            pack_size = db.pack_size(value=item['SKU_Name'])
            if pack_size:
                item['Pack_Size_Local'] = pack_size[1]
                item['Pack_Size'] = pack_size[0]

        # LOCAL PRICE AND USD PRICE
        # price_text = ''.join(response.xpath('//p[@class="card-text text-right"]//button//text()').getall())
        price_text = ''.join(response.xpath(f'//button[@cdshin="{product_id}" and @type="button"]//text()').getall())
        price = re.findall(r'¥([\d,]+)', price_text)
        item['Price_In_Local_USD'] = ''
        if price:
            price = float(max(price).replace(',', ''))
            item['Price_In_Local'] = "%.2f" % price
            item['Price_In_Local_USD'] = "%.2f" % (price * exchange_rate)

        # TASTING NOTES
        item['Tasting_Notes'] = description = response.xpath('//p[@class="card-text"]/text()').get('').strip()

        # AGE OF WHISKEY
        age = re.findall(r'(\d+)年', item['SKU_Name'])
        if not age:
            age = re.findall(r'(\d+)年', description)
        if not age:
            for i in range(1, 80):
                numstring = num2words(i, lang='ja') + '年'
                if numstring in item['SKU_Name']:
                    age = [i]
                    break
        if age and int(age[0]) < 80:
            item['Age_of_Whiskey'] = int(age[0])

        # PACK TYPE
        if '缶' in item['Category'] or '缶' in item['SKU_Name']:
            item['Pack_type'] = 'Can'

            # PACK SIZE NEW
        a = ['NA', '704 ml', '1230 ml', '250 ml', '14.8 cl', '1600 ml', '2150 ml', '2100 ml', '1450 ml', '1980 ml',
             '200 ml', '650 ml', '950 ml', '2.25 l', '2250 ml', '1780 ml', '150 ml', '1580 ml', '50 ml', '1.125 l',
             '1300 ml', '1050 ml', '550 ml', '3500 ml', '1120 ml', '1280 ml', '2450 ml', '701 ml', '70 cl',
             '900 ml',
             '846 ml', '2500 ml', '1350 ml', '2750 ml', '1.75 l', '2200 ml', '2800 ml', '1250 ml', '800 ml',
             '1775 ml',
             '3000 ml', '10 cl', '3000 l', '5 cl', '330 ml', '440 ml', '568 ml', '125 ml', '25 cl', '275 ml',
             '20 cl',
             '180 ml', '33 cl', '250ml', '125ml', '200ml', '100ml', '330ml', '720ml', '20cl', '2640 ml', '5cl',
             '4x250',
             '10x250', '4x5cl', '4.5 l', '1750ml', '375ml', '5250ml', '640ml', '50ml', '300ml', '725ml', '1125ml',
             '275ml', '1136ml', '4500ml', '30 ml', '355ml', '745ml', '400ml', '3000ml', '100 ml', '6 x 375ml',
             '40mL',
             '1450ML', '360mL', '6 x 30mL', '180mL', '650mL', '230mL', '6x50mL', '370mL', '8x50mL', '10330ML',
             '10 x 375mL', '175mL', '3 x 200mL', '12x40mL', '110mL', '475mL', '2330mL', '24375ML', '130mL',
             '1420ML',
             '187mL', '220mL', '4x50mL', '1.125L', '900mL', '20mL', '1.75L', '95mL', '1800mL', '3.4L', '800ML',
             '30mL',
             '3L', '60mL', '80mL', '3500mL', '2.1L', '4x 200mL', '550mL', '3x50ml', '1.125 Litre', '440ml', '270ml',
             '660ml', '1lt', '1l', '510ml', '320ml', '120ml', '140ml', '345ml', '300 l', '225 l', '72 cl',
             '14.6 cl',
             '12 cl', '2 cl', '375 ml', '3.5 cl', '4 cl', '1.75 cl', '500 l', '215 l', '130 l', '1 cl', '47.3 cl',
             '255 l', '115 l', '290 l', '400 l', '125 l', '7.5 cl', '6 cl', '1707 l', '210 l', '44 cl', '37.5 cl',
             '67 cl', '30 cl', '71ml', '0.200l', '660 ml', '300 ml', '7.1 cl', '620 ml', '355 ml', '1100 ml',
             '1320 ml',
             '510 ml', '201 ml', '1.0 cl', '3 l', '300 cl', '75 l', '0.2 l', '100 cl', '50.00 ml', '180.00 ml',
             '720.00 ml', '2100.00 ml', '4000.00 ml', '2760.00 ml', '300.00 ml', '1800.00 ml', '100.00 ml',
             '360.00 ml',
             '200.00 ml', '660.00 ml', '75.00 ml', '475.00 ml', '248.00 ml', '1.8L', '2200ML', '4L', '1920ML',
             '1440ML',
             '8 cl', '23 cl', '1738 cl', '40 ml', '770 ml', '20 ml', '275ml x 6', '275ml x 24', '1.75 Litros',
             '175 Centilitros', '60 Centilitros', '17.5 Centilitros', '35.50 cl', '33 Centilitros', '3 Litros',
             '20 Centilitros', '682.8 g', '187 g', '250 Mililitros', '25 Centilitros']
        if item['Pack_Size_Local'] in a:
            item['Pack_Size_New'] = 'Others'

        # PRICE RANGE
        if item['Price_In_Local_USD']:
            price_in_usd = float(item['Price_In_Local_USD'])
            if price_in_usd < 20: item['Price_Range'] = '<20'
            if price_in_usd >= 20 and price_in_usd <= 30: item['Price_Range'] = '20-30'
            if price_in_usd >= 30 and price_in_usd <= 40: item['Price_Range'] = '30-40'
            if price_in_usd >= 40 and price_in_usd <= 50: item['Price_Range'] = '40-50'
            if price_in_usd >= 50 and price_in_usd <= 60: item['Price_Range'] = '50-60'
            if price_in_usd >= 60 and price_in_usd <= 90: item['Price_Range'] = '60-90'
            if price_in_usd >= 90 and price_in_usd <= 150: item['Price_Range'] = '90-150'
            if price_in_usd > 150: item['Price_Range'] = '150+'

        yield item
        # print(item)

if __name__ == '__main__':
    execute("scrapy crawl data -a start=1 -a end=100000000000000000000000000".split())
