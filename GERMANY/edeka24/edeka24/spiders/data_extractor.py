import json
import os
import re
import datetime
import pymysql
import scrapy
from scrapy.cmdline import execute
import edeka24.db_config as db
from edeka24.items import Edeka24Item
from edeka24.EXPORT import file_generation

class DataExtractorSpider(scrapy.Spider):
    name = "data_extractor"
    allowed_domains = ["edeka24.de"]
    start_urls = ["https://edeka24.de"]
    handle_httpstatus_list = [404]

    # todo this is for pack_size
    def pack_size(self,value):
        Pack_Size = None
        Web_Pack_Size = None
        if re.findall('([-+]?(?:\d*\.*\d+))\s*cl', value, flags=re.IGNORECASE):
            pack = re.findall('([-+]?(?:\d*\.*\d+))\s*cl', value, flags=re.IGNORECASE)
            if pack:
                unit = f'{pack[0]} cl'
                ps = pack[0]
                pack = float(pack[0])
                Pack_Size = round(pack * 10, 2)
                Web_Pack_Size = f'{ps} cl'
        elif re.findall('([-+]?(?:\d*\.*\d+))\s*ml', value, flags=re.IGNORECASE):
            pack = re.findall('([-+]?(?:\d*\.*\d+))\s*ml', value, flags=re.IGNORECASE)
            if pack:
                unit = f'{pack[0]} ml'
                ps = pack[0]
                pack = float(ps)
                Pack_Size = round(pack, 2)
                Web_Pack_Size = f'{ps} ml'
        elif re.findall('([-+]?(?:\d*\.*\d+))\s*l', value, flags=re.IGNORECASE):
            pack = re.findall('([-+]?(?:\d*\.*\d+))\s*l', value, flags=re.IGNORECASE)
            if pack:
                unit = f'{pack[0]} l'
                ps = pack[0]
                pack = float(ps)
                Pack_Size = round(pack * 1000, 2)
                Web_Pack_Size = f'{ps} l'
        if Pack_Size:return Pack_Size,Web_Pack_Size
        else:return None



    def __init__(self, name=None, start=0, end=0, **kwargs):
        # todo this is for database connection and pagesave
        super().__init__(name, **kwargs)
        # DATABASE SPECIFIC VALUES
        self.start = int(start)
        self.end = int(end)
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password)
        self.cursor = self.con.cursor()
        self.data_insert = 0
        self.PAGESAVE = db.PAGESAVE
        if not os.path.exists(self.PAGESAVE):
            os.makedirs(self.PAGESAVE)

        self.sector_dict = {'blended irish whiskey': 'Blended Irish Whiskey',
                            'blended irish whiskey flav.': 'Blended Irish Whiskey',
                            'blend irish whiskey': 'Irish Whiskey', 'irish whiskey': 'Irish Whiskey',
                            'malt irish whiskey': 'Malt Irish Whiskey', 'blended': 'Blended US Whiskey',
                            'bourbon': 'US Whiskey', 'american whiskey': 'US Whiskey', 'other us whiskey': 'US Whiskey',
                            'us whiskey': 'US Whiskey', 'rye': 'US Whiskey', 'tennessee': 'US Whiskey',
                            'blended scotch': 'Blended Scotch Whisky', 'grain scotch': 'Scotch Whisky',
                            'malt scotch': 'Malt Scotch Whisky',
                            'canada': 'Canadian Whisky',
                            'canadian': 'Canadian Whisky',
                            'indian': 'Indian Whisky',
                            'india': 'Indian Whisky',
                            'japanese': 'Japanese Whisky',
                            'japan': 'Japanese Whisky',
                            'usa': 'US Whisky'
                            }

        self.sub_cat_dict = {'blended irish whiskey': 'Irish Whiskey', 'blended irish whiskey flav.': 'Irish Whiskey',
                             'malt irish whiskey': 'Irish Whiskey', 'blended': 'US Whiskey',
                             'blended us whiskey': 'US Whiskey', 'bourbon': 'US Whiskey',
                             'other us whiskey': 'US Whiskey', 'rye': 'US Whiskey', 'tennessee': 'US Whiskey',
                             'blended scotch': 'Scotch Whiskey', 'grain scotch': 'Scotch Whiskey',
                             'malt scotch': 'Scotch Whiskey', 'malt scotch whiskey': 'Scotch Whiskey',
                             'canada': 'Canadian Whisky',
                             'canadian': 'Canadian Whisky',
                             'indian': 'Indian Whisky',
                             'india': 'Indian Whisky',
                             'japanese': 'Japanese Whisky',
                             'japan': 'Japanese Whisky',
                             'usa': 'US Whisky'
                             }

    def start_requests(self):
        # todo this is for fethcing link and request that
        # query = f'select * from {db.db_links_table} where Status="Pending"'
        self.cursor.execute(f"""SELECT * FROM {db.db_links_table} WHERE status = 'pending' AND Id BETWEEN {self.start} and {self.end}""")

        # self.cursor.execute(query)
        query_results = self.cursor.fetchall()
        for product in query_results:
            id = product[0]
            category = product[3]
            product_id = product[2]
            product_url = product[1]
            type= product[5]

            pdp_path = f"{db.PAGESAVE}/{product_id}.html"
            meta_dict = {
                "Id": id,
                "category": category,
                "product_url": product_url,
                "product_id": product_id,
                "type": type,
                "pdp_path": pdp_path
            }
            if os.path.exists(pdp_path):
                print('file:///' + pdp_path)
                yield scrapy.Request(url='file:///' + pdp_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
            else:
                yield scrapy.Request(
                    url=product_url,callback=self.parse,cb_kwargs=meta_dict,dont_filter=True)

    def parse(self, response,**kwargs):
        item = Edeka24Item()
        item['Id'] = kwargs['Id']
        item['Platform_Name'] = 'Edeka24'
        item['Platform_URL'] = kwargs['product_url']
        item['type'] = kwargs['type']
        item['Product_id'] = kwargs['product_id']
        pdp_path = kwargs['pdp_path']
        product_id = kwargs['product_id']
        if not os.path.exists(pdp_path):
            # todo pagesave
            db.pagesave(response, product_id)
        else:
            print("File Already Available...")

        # todo this is for updating status in link table
        if response.status == 404:
            self.cursor.execute(
                f"""UPDATE {db.db_links_table} SET STATUS='404' WHERE product_id='{item['Product_id']}'""")
            self.con.commit()
            return None

        item['Category'] = kwargs['category']

        # filepath = self.PAGE_SAVE_PATH + f'/{item["Product_id"]}.html'
        # with open(filepath, 'a', encoding='utf-8') as f:
        #     f.write(response.text)
        #     f.close()

        # todo Extract SKU name from the product page
        try:
            sku_name = response.xpath('//h1//text()').get().strip()
        except:
            sku_name = ''
        # todo Extract ABV (Alcohol by Volume) from the product page
        try:
            abv = response.xpath('//*[contains(text(),"Alkoholgehalt")]/following::text()[1]').get().strip()
        except:
            abv = ''
        # todo Extract country of origin from the product page
        try:
            item['Country_of_Origin'] = response.xpath('//*[contains(text(),"Herkunft")]/following-sibling::td//text()').get().strip()
        except:
            item['Country_of_Origin'] = 'NA'

        #todo Check if ABV is available, if not try to extract it from SKU name
        if abv:
            try:
                item['ABV'] = re.findall('(\d+)%', abv.replace(',', '.'))[0].strip()
            except:
                try:
                    item['ABV'] = re.findall('(\d+.\d+)%', abv.replace(',', '.'))[0].strip()
                except:
                    item['ABV'] = 'NA'
        else:
            try:
                item['ABV'] = re.findall('(\d+)%', sku_name.lower().replace(',', '.'))[0].strip()
            except:
                try:
                    item['ABV'] = re.findall('(\d+.\d+)%', sku_name.lower().replace(',', '.'))[0].strip()
                except:
                    item['ABV'] = 'NA'

        # todo extracing pack_size
        try:
            pack_size = re.findall('(\d+,\d+l)', sku_name.lower())[0]
        except:
            pack_size = ''

        if pack_size:
            item['Pack_Size_Local'] = pack_size
            try:
                pack_size = re.findall('(\d+.\d+cl)', item['Pack_Size_Local'].lower().replace(',', '.'))
                if not pack_size:
                    pack_size = re.findall('(\d+.\d+ml)', item['Pack_Size_Local'].lower().replace(',', '.'))
                if not pack_size:
                    pack_size = re.findall('(\d+.\d+litre)', item['Pack_Size_Local'].lower().replace(',', '.'))
                if not pack_size:
                    pack_size = re.findall('(\d+.\d+l)', item['Pack_Size_Local'].lower().replace(',', '.'))
                if pack_size:
                    item['Pack_Size_Local'] = pack_size[0].strip().replace('.', ',')
                    item['Pack_Size'] = pack_size[0].strip().lower()
                    if 'x' in item['Pack_Size']:
                        item['Pack_Size'] = item['Pack_Size'].split('x')[1].strip()
                    if 'cl' in item['Pack_Size']:
                        item['Pack_Size'] = str(int(float(item['Pack_Size'].replace('cl', '')) * 10))
                    elif 'ml' in item['Pack_Size']:
                        item['Pack_Size'] = str(int(float(item['Pack_Size'].replace('ml', ''))))
                    elif 'litre' in item['Pack_Size']:
                        item['Pack_Size'] = str(int(float(item['Pack_Size'].replace('litre', '')) * 1000))
                    elif 'lt' in item['Pack_Size']:
                        item['Pack_Size'] = str(int(float(item['Pack_Size'].replace('lt', '')) * 1000))
                    elif 'l' in item['Pack_Size']:
                        item['Pack_Size'] = str(int(float(item['Pack_Size'].replace('l', '')) * 1000))
                else:
                    item['Pack_Size'] = 'NA'
            except:
                item['Pack_Size'] = 'NA'
        else:
            item['Pack_Size_Local'] = 'NA'
            item['Pack_Size'] = 'NA'
        if item['Pack_Size'] == 'NA':
            pack = self.pack_size(sku_name)
            if pack:
                item['Pack_Size_Local'] = pack[1]
                item['Pack_Size'] = pack[0]
        item['SKU_Name'] = sku_name
        # todo this is for pack_size_local & ABV
        if item['Pack_Size_Local'] != 'NA':
            item['SKU_Name'] = sku_name.lower().replace(item['Pack_Size_Local'], '').strip().title()
        if item['ABV'] != 'NA':
            item['SKU_Name'] = sku_name.lower().replace(item['SKU_Name'], '').strip().title()


        # todo this is for price_in_local,price_range
        item['Price_in_Local']= 'NA'
        item['Price_in_Local_USD'] = 'NA'
        try:
            price = response.xpath('//*[@id="jq_widgetContainer_articleDetailsPrice"]//*[@class="price"]//text()').get().strip().replace('€', '')
        except:
            try:
                price = response.xpath('//*[@id="jq_widgetContainer_articleDetailsPrice"]//*[@class="price salesprice"]//text()').get().strip().replace('€', '')
            except:
                price = ''

        if price:
            item['Price_in_Local'] = price.strip().replace(',','.')
            item['Price_in_Local_USD'] = db.convert_currency('EUR', float(price.replace(',', '.')))
        else:
            item['Price_in_Local'] = 'NA'
            item['Price_in_Local_USD'] = 'NA'

        item['Price_Range'] = 'NA'
        if item['Price_in_Local_USD'] != 'NA':
            price_in_usd = float(item['Price_in_Local_USD'])
            if price_in_usd < 20: item['Price_Range'] = '<20'
            if price_in_usd >= 20 and price_in_usd <= 30: item['Price_Range'] = '20-30'
            if price_in_usd >= 30 and price_in_usd <= 40: item['Price_Range'] = '30-40'
            if price_in_usd >= 40 and price_in_usd <= 50: item['Price_Range'] = '40-50'
            if price_in_usd >= 50 and price_in_usd <= 60: item['Price_Range'] = '50-60'
            if price_in_usd >= 60 and price_in_usd <= 90: item['Price_Range'] = '60-90'
            if price_in_usd >= 90 and price_in_usd <= 150: item['Price_Range'] = '90-150'
            if price_in_usd > 150: item['Price_Range'] = '150+'
        # todo this is for price_in_local
        unit_price = response.xpath('//*[@class="price-wrap"]//*[contains(text(),"€/")]/text()').get()
        if unit_price:
            unit_price = float(re.findall('([-+]?(?:\d*[\,|\.]*\d+))',unit_price,flags=re.IGNORECASE)[0].replace(',','.'))
            item['Price_per_unit_Local'] = unit_price
            item['Price_per_unit_USD'] = db.convert_currency('EUR', float(price.replace(',', '.')))

        if item['Price_per_unit_USD']:
            item['Price_per_unit_USD_New']=item['Price_per_unit_USD']
        else:item['Price_per_unit_USD']=''
        #todo this is for age_of_whisky
        try:
            item['Age_of_Whisky'] = re.findall('(\d+) jahre', item['SKU_Name'].lower())[0].strip()
        except:
            item['Age_of_Whisky'] = 'NA'

        # todo image_url
        try:
            images = response.xpath('//*[@class="detail-image"]//img/@src').getall()
            if images:
                item['Image_Urls'] = ' | '.join(images)
            else:
                item['Image_Urls'] = 'NA'
        except:
            item['Image_Urls'] = 'NA'

        # todo this is for pack_size_new
        if item['SKU_Name']:
            if item['Image_Urls'] != 'NA':
                item['Pack_Size_New'] = 'NA'
                a = ['NA', '704 ml', '1230 ml', '250 ml', '14.8 cl', '1600 ml', '2150 ml', '2100 ml', '1450 ml',
                     '1980 ml', '200 ml', '650 ml', '950 ml', '2.25 l', '2250 ml', '1780 ml', '150 ml', '1580 ml',
                     '50 ml', '1.125 l', '1300 ml', '1050 ml', '550 ml', '3500 ml', '1120 ml', '1280 ml', '2450 ml',
                     '701 ml', '70 cl', '900 ml', '846 ml', '2500 ml', '1350 ml', '2750 ml', '1.75 l', '2200 ml',
                     '2800 ml', '1250 ml', '800 ml', '1775 ml', '3000 ml', '10 cl', '3000 l', '5 cl', '330 ml',
                     '440 ml', '568 ml', '125 ml', '25 cl', '275 ml', '20 cl', '180 ml', '33 cl', '250ml', '125ml',
                     '200ml', '100ml', '330ml', '720ml', '20cl', '2640 ml', '5cl', '4x250', '10x250', '4x5cl', '4.5 l',
                     '1750ml', '375ml', '5250ml', '640ml', '50ml', '300ml', '725ml', '1125ml', '275ml', '1136ml',
                     '4500ml', '30 ml', '355ml', '745ml', '400ml', '3000ml', '100 ml', '6 x 375ml', '40mL', '1450ML',
                     '360mL', '6 x 30mL', '180mL', '650mL', '230mL', '6x50mL', '370mL', '8x50mL', '10330ML',
                     '10 x 375mL', '175mL', '3 x 200mL', '12x40mL', '110mL', '475mL', '2330mL', '24375ML', '130mL',
                     '1420ML', '187mL', '220mL', '4x50mL', '1.125L', '900mL', '20mL', '1.75L', '95mL', '1800mL', '3.4L',
                     '800ML', '30mL', '3L', '60mL', '80mL', '3500mL', '2.1L', '4x 200mL', '550mL', '3x50ml',
                     '1.125 Litre', '440ml', '270ml', '660ml', '1lt', '1l', '510ml', '320ml', '120ml', '140ml', '345ml',
                     '300 l', '225 l', '72 cl', '14.6 cl', '12 cl', '2 cl', '375 ml', '3.5 cl', '4 cl', '1.75 cl',
                     '500 l', '215 l', '130 l', '1 cl', '47.3 cl', '255 l', '115 l', '290 l', '400 l', '125 l',
                     '7.5 cl', '6 cl', '1707 l', '210 l', '44 cl', '37.5 cl', '67 cl', '30 cl', '71ml', '0.200l',
                     '660 ml', '300 ml', '7.1 cl', '620 ml', '355 ml', '1100 ml', '1320 ml', '510 ml', '201 ml',
                     '1.0 cl', '3 l', '300 cl', '75 l', '0.2 l', '100 cl', '50.00 ml', '180.00 ml', '720.00 ml',
                     '2100.00 ml', '4000.00 ml', '2760.00 ml', '300.00 ml', '1800.00 ml', '100.00 ml', '360.00 ml',
                     '200.00 ml', '660.00 ml', '75.00 ml', '475.00 ml', '248.00 ml', '1.8L', '2200ML', '4L', '1920ML',
                     '1440ML', '8 cl', '23 cl', '1738 cl', '40 ml', '770 ml', '20 ml', '275ml x 6', '275ml x 24',
                     '1.75 Litros', '175 Centilitros', '60 Centilitros', '17.5 Centilitros', '35.50 cl',
                     '33 Centilitros', '3 Litros', '20 Centilitros', '682.8 g', '187 g', '250 Mililitros',
                     '25 Centilitros']
                if item['Pack_Size_Local'] in a:
                    item['Pack_Size_New'] = 'Others'

        item['Country'] = 'Germany'
        item['Major_Region'] = 'Europe'
        item['Market_Clusters'] = 'EMEA Develop'
        item['Standard_Currency'] = 'USD'

        item['scrape_date'] = datetime.datetime.now().strftime("%d_%m_%Y")

        yield item
        # print(item)

    def closed(self, reason):
        """This method is called when the spider finishes."""
        print("Spider closed, reason:", reason)
        file_generation()  # Call your file generation function here


if __name__ == '__main__':
    execute('scrapy crawl data_extractor -a start=1 -a end=10000000000000000'.split())
    # execute('scrapy crawl data_extractor -a start=1 -a end=1'.split())
