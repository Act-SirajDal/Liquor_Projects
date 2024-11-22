import os
import re
from datetime import datetime
import pymysql
import scrapy
from scrapy.cmdline import execute
import Lidl.db_config as db
from Lidl.EXPORT import file_generation
from Lidl.items import Lidl_dataItem


class DataSpider(scrapy.Spider):
    name = 'data_extractor'
    allowed_domains = ['www.lidl.de']
    # start_urls = ['http://www.lidl.de/']
    handle_httpstatus_list = [404]

    def __init__(self,start=0,end=0):
        # todo this is for database connetion and fetch and links and pagesave
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
        # self.pagesave = 'D:/Page_save/Lidl/'


    def start_requests(self):
        for i in self.all_d:
            Platform_URL = i[1]
            Product_id = i[2]
            type = i[4]
            pdp_path = f"{db.PAGESAVE}/{Product_id}.html"
            meta_dict = {
                "Platform_URL": Platform_URL,
                "Product_id": Product_id,
                "type": type,
                "pdp_path": pdp_path
            }
            if os.path.exists(pdp_path):
                print('file:///' + pdp_path)
                yield scrapy.Request(url='file:///' + pdp_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
            else:
                yield scrapy.Request(url=Platform_URL,callback=self.parse,cb_kwargs=meta_dict,dont_filter=True)

    def parse(self, response,**kwargs):
        item = Lidl_dataItem()
        item['Platform_URL']=kwargs['Platform_URL']
        item['type']=kwargs['type']
        Product_id = kwargs['Product_id']
        item['Product_id'] =Product_id
        pdp_path = kwargs['pdp_path']

        if "Not found" not in response.text:
            if not os.path.exists(pdp_path):
                # todo pagesave
                db.pagesave(response, Product_id)
            else:
                print("File Already Available...")

            # todo updating an status in link table
            if response.status == 404:
                self.cursor.execute(
                    f"""UPDATE {db.db_links_table} SET STATUS='404' WHERE product_id='{item['Product_id']}'""")
                self.con.commit()
                return None

            # open(self.pagesave + str(Product_id) + ".html", "wb").write(response.body)


            # todo extracting sku_name
            SKU_Name = response.xpath('//h1[@data-qa-label="keyfacts-title"]/text()').get()
            SKU_Name = re.sub(' +', ' ', SKU_Name).strip()
            item['SKU_Name'] = SKU_Name


            item['Age_of_Whiskey'] = ""
            item['Pack_Size_Local'] = 'NA'
            item['Manufacturer'] = "NA"
            item['Distillery'] = "NA"
            item['ABV'] = "NA"
            item['Category'] = "NA"
            item['Country_of_Origin'] = "NA"

            # todo this is for category, ABV, Country_of_Origin,Age_of_Whiskey,Pack_Size_Local
            pack = response.xpath('//ul[@class="attributes"]//li')
            if pack:
                for i in pack:
                    key1 = i.xpath('.//strong/text()').get()
                    key = key1.strip()
                    if key == 'Kategorie:':
                        Category = i.xpath('.//span//text()').get()
                        item['Category'] = Category

                    elif key == 'Alkoholgehalt:':
                        value = i.xpath('.//span//text()').get()
                        string = value
                        pattern = r"(\d+(?:,\d+)?)\s*%"
                        match = re.search(pattern, string)
                        if match:
                            ABV = match.group(1)
                            item['ABV'] = ABV.replace(',','.')
                            # print(match.group(1))

                        # ABV =value.split('%')[0].replace(',','.')
                        # item['ABV'] = ABV

                    elif key == 'Herkunftsland:':
                        Country_of_Origin = i.xpath('.//span//text()').get()
                        item['Country_of_Origin'] = Country_of_Origin

                    elif key == 'Alter/Reife:':
                        value1 =i.xpath('.//span//text()').get()
                        string = value1
                        pattern = r"\d+\s*Jahre"

                        match = re.search(pattern, string)
                        if match:
                            years = match.group()
                            item['Age_of_Whiskey'] = years.replace('Jahre', '')
                            print(years)
                        # else:
                        #     string = SKU_Name
                        #     pattern = r"\d+\s*Jahre"
                        #
                        #     match = re.search(pattern, string)
                        #     if match:
                        #         years = match.group()
                        #         item['Age_of_Whiskey'] = years.replace('Jahre', '')
                        #         print(years)

                    elif key == 'Gebindegröße:':
                        value2= i.xpath('.//span//text()').get()
                        string = value2
                        pack_size_pattern = r"\d+(,\d+)?\s*(-l|l|ml)"
                        pack_type_pattern = r"(?<=-)(\w+)$"
                        pack_size_match = re.search(pack_size_pattern, string)
                        if pack_size_match:
                            pack_size1 = pack_size_match.group()
                            Pack_Size_Local= pack_size1.replace(',','.')
                            item['Pack_Size_Local'] = Pack_Size_Local.replace('-',' ')
                            print(Pack_Size_Local)

                            Pack_Sizes = Pack_Size_Local
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


                            pack_type_match = re.search(pack_type_pattern, string)
                            if pack_type_match:
                                Pack_type= pack_type_match.group()
                                item['Pack_type'] = Pack_type

                                print(Pack_type)

                    # elif key == 'Farbe:':
                    #     test = i.xpath('.//span//text()').get()
                    #     dictlist = []
                    #
                    #     if key:
                    #         key = key.replace(':', '')
                    #         value = test
                    #         dictlist.append(f'{key}:{value}')
                    #         Tasting_Notes = ','.join(dictlist)
                    #         Tasting_Notes = re.sub(' +', ' ', Tasting_Notes).strip()
                    #
                    #         item['Tasting_Notes'] = Tasting_Notes

                    # todo this is for Distillery, Manufacturer
                    elif key == 'Hersteller:':
                        value3= i.xpath('.//span//text()').get()
                        if "destillerie" in value3:
                            Distillery1 = value3
                            Distillery = Distillery1.split('destillerie')[1].split(',')[0]
                            item['Distillery'] = Distillery.strip()
                            Manufacturer = Distillery
                            item['Manufacturer'] = Manufacturer.strip()
                        else:
                            Manufacturers1 = value3
                            Manufacturer = Manufacturers1.split(',')[0]
                            item['Manufacturer'] = Manufacturer.strip()

            # todo this is Pack_Size_Local, Pack_Size
            aa = re.findall(r'([-+]?(?:\d*[\.|\,]*\d+))\s*X\s*([-+]?(?:\d*[\.|\,]*\d+))\s*[-]?\s*([a-z]+)',item['SKU_Name'].replace(',', '.'), flags=re.IGNORECASE)
            if aa:
                pack = str(float(aa[0][0]) * float(aa[0][1]))
                pack1 = "%.2f" % float(pack)
                pack2 = pack1 + aa[0][2]
                print(pack2)

                item['Pack_Size_Local'] = pack2

                Pack_Sizes = pack2
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


            string = SKU_Name
            pack_type_pattern = r"\b(Flasche|Bottle)\b"
            pack_type_match = re.search(pack_type_pattern, string)
            if pack_type_match:
                Pack_type = pack_type_match.group()
                item['Pack_type'] = Pack_type
                print(Pack_type)

            # todo this is for Pack_Size_New, Age_of_Whiskey
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

            if not item['Age_of_Whiskey']:
                string = SKU_Name
                pattern = r"\d+\s*(Jahre|Year)"

                match = re.search(pattern, string)
                if match:
                    years = match.group()
                    item['Age_of_Whiskey'] = years.replace('Jahre', '').replace('Year','').strip()
                    print(years)

            # todo this is for Price_In_Local, Promo_Price_Local
            item['Price_In_Local'] = "NA"
            item['Price_In_Local_USD'] = "NA"
            item['Promo_Price_Local'] = "NA"
            item['Promo_Price_USD'] = "NA"

            Price_In_Locals = response.xpath('//div[@class="m-price__top"]//span//text()').get()
            if Price_In_Locals:
                Price_In_Local = float(Price_In_Locals.strip().replace('*',''))
                Price_In_Local1 = "%.2f" % Price_In_Local
                item['Price_In_Local'] = Price_In_Local1

                Price_In_Local_USD1 = float(Price_In_Local) * 1.1500
                Price_In_Local_USD = "%.2f" % Price_In_Local_USD1
                item['Price_In_Local_USD'] = Price_In_Local_USD

                Promo_Price_Locals = response.xpath('//div[@class="m-price__price"]//text()').get()
                if Promo_Price_Locals:
                    Promo_Price_Local1 = float(Promo_Price_Locals.strip())
                    Promo_Price_Local = "%.2f" % Promo_Price_Local1
                    item['Promo_Price_Local'] = Promo_Price_Local

                    Promo_Price_USD1 = float(Promo_Price_Local) * 1.1500
                    Promo_Price_USD = "%.2f" % Promo_Price_USD1
                    item['Promo_Price_USD'] = Promo_Price_USD

            else:
                Price_In_Locals = response.xpath('//div[@class="m-price__price"]//text()').get()
                Price_In_Local =float(Price_In_Locals.strip())
                Price_In_Local2 = "%.2f" % Price_In_Local
                item['Price_In_Local'] = Price_In_Local2

                Price_In_Local_USD1 = float(Price_In_Local) * 1.1500
                Price_In_Local_USD = "%.2f" % Price_In_Local_USD1
                item['Price_In_Local_USD'] = Price_In_Local_USD

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

            # todo this is for Price_per_unit_Local,Promo_Price_Local
            item['Price_per_unit_Local'] = "NA"
            item['Price_per_unit_USD'] = "NA"
            Price_per_unit_Local1 = ''.join(response.xpath('//div[@class="buybox__price"]//div[@class="price-footer"]/text()').getall()).strip()
            if Price_per_unit_Local1:
                Price_per_unit_Local2 = float(''.join(Price_per_unit_Local1).strip().split('=')[1])
                Price_per_unit_Local = "%.2f" % Price_per_unit_Local2
                item['Price_per_unit_Local'] =Price_per_unit_Local

                Price_per_unit_USD1 = Price_per_unit_Local2 *1.1500
                Price_per_unit_USD = "%.2f" % Price_per_unit_USD1
                item['Price_per_unit_USD'] = Price_per_unit_USD
            else:
                Price_per_unit_Local = response.xpath('//small[@class="price__base"]//text()').get('')
                if 'je 1-l' in Price_per_unit_Local:
                    if item['Promo_Price_Local'] != 'NA':
                        item['Price_per_unit_Local'] = item['Promo_Price_Local']
                        item['Price_per_unit_USD'] = item['Promo_Price_USD']
                    elif item['Price_In_Local'] != 'NA':
                        item['Price_per_unit_Local'] = item['Price_In_Local']
                        item['Price_per_unit_USD'] = item['Price_In_Local_USD']
                else:
                    item['Price_per_unit_Local'] = "NA"
                    item['Price_per_unit_USD'] = "NA"

            if item['Price_per_unit_USD']:
                item['Price_per_unit_USD_New']=item['Price_per_unit_USD']
            else:item['Price_per_unit_USD_New']=''

            # except :
            #     item['Price_per_unit_Local'] = 'NA'
                # print(e,response.url)

            # item['Distillery'] = "NA"
            # Manufacturers = response.xpath('//li[@class="spirits producer"]//span//text()').get('')
            # if "destillerie" in Manufacturers:
            #     Distillery1 = Manufacturers
            #     Distillery = Distillery1.split('destillerie')[1].split(',')[0]
            #     item['Distillery'] = Distillery
            #     Manufacturer = Distillery
            #     item['Manufacturer'] = Manufacturer
            # else:
            #     Manufacturers = Manufacturers
            #     item['Manufacturer'] = Manufacturers


                # Distillery = Manufacturers.split('destillerie')[1].split(',')[0]


            # todo this is for image
            img = response.xpath('//div[@id="product-multimediabox-gallery"]//img//@src').getall()
            img1 = set(img)
            img_url = '|'.join(img1)
            item['Image_Urls'] = img_url

            current_date = datetime.now()
            scrape_date = current_date.strftime("%d_%m_%Y")
            item['scrape_date'] = scrape_date
            item['Platform_Name'] = "Lidl"
            item['Brand'] = "NA"
            item['Sub_Brand'] = "NA"
            item['Sub_Category'] = "NA"
            item ['Sector'] = "NA"
            item['Sub_Sector'] = "NA"
            item['Origin'] = "NA"
            item['Major_Region'] = "Europe"
            item['Country'] = "Germany"
            item['Market_Clusters'] = "EMEA Develop"
            item['Type_of_Promo'] = "NA"
            item['Tasting_Notes'] = "NA"
            item['Standard_Currency'] ="USD"

            yield item
            # print(item)
        else:
            update_Status = f'''UPDATE {self.db_table} set status="Not Found" Where product_id={Product_id}'''
            self.cur.execute(update_Status)
            self.conn.commit()


    def closed(self, reason):
        """This method is called when the spider finishes."""
        print("Spider closed, reason:", reason)
        file_generation()  # Call your file generation function here

if __name__ == '__main__':
    # execute("scrapy crawl data_extractor -a start=1 -a end=10000000000".split())
    execute("scrapy crawl data_extractor -a start=1 -a end=1".split())

