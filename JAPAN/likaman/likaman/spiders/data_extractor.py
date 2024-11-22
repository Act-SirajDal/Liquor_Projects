import os
import re
from datetime import datetime
from likaman.EXPORT import file_generation
import pymysql
import scrapy
from scrapy.cmdline import execute
import likaman.db_config as db
from likaman.items import LikamanItem

class DataSpider(scrapy.Spider):
    name = "data_extractor"
    allowed_domains = ["www.likaman-online.com"]
    handle_httpstatus_list = [404]

    def __init__(self ,start=0, end=0,):
        self.start = start
        self.end = end
        self.con = pymysql.connect(host= db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
        self.cursor = self.con.cursor(pymysql.cursors.DictCursor)

    def start_requests(self):
        self.cursor.execute(f"SELECT * FROM {db.db_links_table} WHERE status = 'pending' and id between {self.start} and {self.end}""")
        links = self.cursor.fetchall()

        for link in links:
            product_id = link['product_id']
            product_url = link['url']
            Type = link['Type']
            id = link['Id']
            category= link['category']
            pdp_path = f"{db.PAGESAVE}/{product_id}.html"
            meta_dict = {
                "url": product_url,
                "id": id,
                "product_id": product_id,
                "Type": Type,
                "category" : category,
                "pdp_path": pdp_path
            }
            if os.path.exists(pdp_path):
                yield scrapy.Request(url=f'file:///{pdp_path}',callback=self.parse,dont_filter=True,cb_kwargs=meta_dict)
            else:
                yield scrapy.Request(url=product_url,callback=self.parse,dont_filter=True,cb_kwargs=meta_dict)

    def parse(self,response,**kwargs):
        pdp_path = kwargs['pdp_path']
        Product_id= kwargs['product_id']
        item = LikamanItem()
        exchange_rate = 0.0068
        item['Id'] = kwargs['id']
        item['Type'] = kwargs['Type']
        item['Platform_Name'] = 'Likaman'
        item['Standard_Currency'] = 'USD'
        item['Major_Region'] = 'Asia Pacific'
        item['Country'] = 'Japan'
        item['Market_Clusters'] = 'APAC Develop'
        item['scrape_date'] = datetime.today().strftime('%d_%m_%Y')
        item['Platform_URL'] = kwargs['url']
        item['Product_id'] = kwargs['product_id']
        item['Category'] = kwargs['category']

        # PAGESAVE
        # db.pagesave(path=db.PAGESAVE, response=response, product_id=item['Product_id'])
        if not os.path.exists(pdp_path):
            # todo pagesave
            db.pagesave(response, Product_id)
        else:
            print("File Already Available...")

        if response.status == 404:
            self.cursor.execute(f"""UPDATE {db.db_links_table} SET STATUS='404' WHERE product_id='{item['Product_id']}'""")
            self.con.commit()
            return None

        item['SKU_Name'] = re.sub(r'\s+', ' ', ' '.join(response.xpath('//h1/span[@class="fs-c-productNameHeading__name"]//text()').getall())).strip()
        item['SKU_Name_2'] = response.xpath('//*[contains(text(),"商品名")]/following-sibling::td/text()').get('').strip()
        item['Brand'] = response.xpath('//li[@class="fs-c-breadcrumb__listItem"]/a/@href').getall()[-1].split('/')[-1].capitalize()
        # item['Origin'] = item['Country_of_Origin'] = response.xpath('//*[contains(text(),"原産国・生産国/生産地") or contains(text(),"アルコール度")]/following-sibling::td/text()').get()
        item['Origin'] = item['Country_of_Origin'] = response.xpath('//*[contains(text(),"原産国・生産国/生産地") or contains(text(),"産地")]/following-sibling::td/text()').get('').strip()


        # PACK SIZE
        item['Pack_Size_Local'] = 'NA'
        item['Pack_Size'] = 'NA'
        pack_size = db.pack_size(value=item['SKU_Name'])
        packsize2 = response.xpath('//*[contains(text(),"容量")]/following-sibling::td/text()').get()
        packsize3 = ''.join(response.xpath('//div[@class="product_comments_content_desc_text"]//text()').getall())
        packsize4 = ''.join(response.xpath('//div[@class="product_comments_content_intro"]//text()').getall())
        if pack_size:
            item['Pack_Size_Local'] = pack_size[1]
            item['Pack_Size'] = pack_size[0]
        elif packsize2:
            pack_size = db.pack_size(value=packsize2)
            if pack_size:
                item['Pack_Size_Local'] = pack_size[1]
                item['Pack_Size'] = pack_size[0]
        elif packsize3:
            packsize = re.findall(r'(\d+)\s*(ml|cl|l)\s*(\d+)度', packsize3)
            if packsize:
                pack_size = db.pack_size(value=packsize[0][0]+packsize[0][1])
                if pack_size:
                    item['Pack_Size_Local'] = pack_size[1]
                    item['Pack_Size'] = pack_size[0]
                    item['ABV'] = packsize[0][2]
        elif packsize4:
            packsize = re.findall(r'(\d+)\s*(ml|cl|l)\s*／\s*(\d+)%', packsize4)
            if packsize:
                pack_size = db.pack_size(value=packsize[0][0] + packsize[0][1])
                if pack_size:
                    item['Pack_Size_Local'] = pack_size[1]
                    item['Pack_Size'] = pack_size[0]
                    item['ABV'] = packsize[0][2]


        # ALCOHOL BY VOLUME
        abv = response.xpath('//*[contains(text(),"アルコール度数") or contains(text(),"アルコール度")]/following-sibling::td/text()').get('')
        abv = re.findall('([-+]?(?:\d*\.*\d+))', abv, flags=re.IGNORECASE)
        if abv:
            item['ABV'] = abv[0] if abv[0] != '0' else None

        # ALCOHOL TYPE
        # item['type'] = response.xpath('//*[contains(text(),"タイプ")]/following-sibling::td/text()').get()

        # PRICE
        price = float(response.xpath('//div[@class="fs-c-productPrices fs-c-productPrices--productDetail"]//span[@class="fs-c-price__value"]/text()').get('').replace(',', ''))
        item['Price_In_Local'] = "%.2f" % price
        item['Price_In_Local_USD'] = "%.2f" % (price * exchange_rate)

        # AGE OF WHISKEY
        age = re.findall(r'(\d+)年 ', item['SKU_Name'], flags=re.IGNORECASE)
        if age:
            age = age[0]
            if int(age) < 200:
                item['Age_of_Whiskey'] = age
        else:
            description = ' '.join(response.xpath('//div[@class="product_comments_content_desc_text"]/div/p[1]//text()').getall())
            age = re.findall(r'(\d+)年間熟成', description, flags=re.IGNORECASE)
            if age:
                age = age[0]
                if int(age) < 200:
                    item['Age_of_Whiskey'] = age

        # TASTING NOTES
        description = ','.join(response.xpath('//div[@class="product_comments_content_desc_text"]/div/p[1]//text()').getall()).replace('\r', '').replace('\n', '')
        taste_notes = re.findall("【テイスティングノート】(.*)", description, flags=re.IGNORECASE)
        if taste_notes: item['Tasting_Notes'] = taste_notes[0].strip()

        # IMAGE URLS
        item['Image_Urls'] = ' | '.join(response.xpath('//figure/img/@src').getall())

        # PACK TYPE
        # if '瓶' in item['SKU_Name'] or '本' in item['SKU_Name']:
        #     item['Pack_type'] = 'Bottle'
        item['Pack_type'] = 'Bottle' if any(x in item['SKU_Name'] for x in ['瓶', '本']) else None

        # PACK SIZE NEW
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

    def closed(self, reason):
        """This method is called when the spider finishes."""
        print("Spider closed, reason:", reason)
        file_generation()  # Call your file generation function here


if __name__ == '__main__':
    # execute("scrapy crawl data_extractor -a start=1 -a end=1000000000000000000000".split())
    execute("scrapy crawl data_extractor -a start=1 -a end=1".split())