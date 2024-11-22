import json
import os.path
import re
from datetime import datetime
import glovoapp.db_config as db
import scrapy
from scrapy.cmdline import execute
from scrapy.utils.response import open_in_browser
from glovoapp.items import Glovoapp_dataItem
from glovoapp.EXPORT import file_generation

class LinkSpider(scrapy.Spider):
    name = 'link'
    allowed_domains = ['glovoapp.com']

    def start_requests(self):
        # urls = 'https://glovoapp.com/bg/en/sofia/fantastiko-sof/?content=napitki-sc.242640611%2Fvisokoalkoholni-napitki-c.1456463536'
        urls = 'https://glovoapp.com/bg/en/sofia/billa-sof1/?content=napitki-sc.373843599%2Fvisokoalkoholni-napitki-c.2162070928'

        cookies = {
            'glovo_user_lang': 'en',
            'glovo_last_visited_city': 'SOF',
            'glv_device': '%257B%2522id%2522%253A1440806416%252C%2522urn%2522%253A%2522glv%253Adevice%253Ad8c64f45-5833-4f74-97d9-3c62d8a18483%2522%257D',
            'ga4_gtag_ga': 'GA1.1.874380759.1694239522',
            '_gid': 'GA1.2.1464363605.1694239525',
            'ab.storage.deviceId.2c08ee6c-f2fe-438a-bc41-8c7d8a2d4d7e': '%7B%22g%22%3A%2281d02884-d927-e1a9-c1cc-27328aea3524%22%2C%22c%22%3A1694239550458%2C%22l%22%3A1694239550458%7D',
            '_fbp': 'fb.1.1694239551061.658276294',
            'OptanonAlertBoxClosed': '2023-09-11T08:55:01.910Z',
            'OptanonConsent': 'isIABGlobal=false&datestamp=Mon+Sep+11+2023+14%3A25%3A02+GMT%2B0530+(India+Standard+Time)&version=6.19.0&hosts=&consentId=72ce82b0-c68a-4789-8636-28fb688c3448&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0005%3A1%2CC0004%3A1%2CC0003%3A1&geolocation=AU%3BWA&AwaitingReconsent=false',
            '_ga_JQ8WY8QVVL': 'GS1.1.1694422502.8.0.1694422502.0.0.0',
            '_ga': 'GA1.1.874380759.1694239522',
            'ga4_gtag_ga_1SYVCWEQ60': 'GS1.1.1694422502.6.0.1694422567.0.0.0',
            'ga4_gtag_ga_K03Y18HZ25': 'GS1.1.1694422502.6.0.1694422567.0.0.0',
            '_ga_LJB7LPW0D9': 'GS1.1.1694422502.7.1.1694422852.0.0.0',
            '_dd_s': '',
        }

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            # 'Cookie': 'glovo_user_lang=en; glovo_last_visited_city=SOF; glv_device=%257B%2522id%2522%253A1440806416%252C%2522urn%2522%253A%2522glv%253Adevice%253Ad8c64f45-5833-4f74-97d9-3c62d8a18483%2522%257D; ga4_gtag_ga=GA1.1.874380759.1694239522; _gid=GA1.2.1464363605.1694239525; ab.storage.deviceId.2c08ee6c-f2fe-438a-bc41-8c7d8a2d4d7e=%7B%22g%22%3A%2281d02884-d927-e1a9-c1cc-27328aea3524%22%2C%22c%22%3A1694239550458%2C%22l%22%3A1694239550458%7D; _fbp=fb.1.1694239551061.658276294; OptanonAlertBoxClosed=2023-09-11T08:55:01.910Z; OptanonConsent=isIABGlobal=false&datestamp=Mon+Sep+11+2023+14%3A25%3A02+GMT%2B0530+(India+Standard+Time)&version=6.19.0&hosts=&consentId=72ce82b0-c68a-4789-8636-28fb688c3448&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0005%3A1%2CC0004%3A1%2CC0003%3A1&geolocation=AU%3BWA&AwaitingReconsent=false; _ga_JQ8WY8QVVL=GS1.1.1694422502.8.0.1694422502.0.0.0; _ga=GA1.1.874380759.1694239522; ga4_gtag_ga_1SYVCWEQ60=GS1.1.1694422502.6.0.1694422567.0.0.0; ga4_gtag_ga_K03Y18HZ25=GS1.1.1694422502.6.0.1694422567.0.0.0; _ga_LJB7LPW0D9=GS1.1.1694422502.7.1.1694422852.0.0.0; _dd_s=',
            'If-None-Match': '"7f558-oyKSO6af/tQh6gK2daugl9oLoBs"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        # pl_file_path = fr"C:\PAGESAVE\METRO\{db.month_date}\product_listing.html"
        pl_file_path = fr"{db.PAGESAVE}/product_listing.html"
        meta_dict = {"pl_file_path": pl_file_path}
        if os.path.exists(pl_file_path):
            yield scrapy.Request(url='file:///' + pl_file_path,cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
        else:
            # yield scrapy.Request(url=urls,callback=self.parse,cb_kwargs=meta_dict,dont_filter=True)
            yield scrapy.Request(url=urls,headers=headers,cookies=cookies,callback=self.parse,cb_kwargs=meta_dict,dont_filter=True)

    def parse(self,response,**kwargs):
        pattern = r'data:{id:(\d+)'
        id_lists = re.findall(pattern, response.text)
        pl_file_path = kwargs['pl_file_path']
        # open_in_browser(response)
        if id_lists:
            if not os.path.exists(pl_file_path):
                product_id_name = "product_listing"
                db.pagesave(response,product_id_name)
            else:
                print("File Already Available....")
            for i in id_lists:
                id = i
                print(i)
                # url = f"https://api.glovoapp.com/v3/stores/195389/addresses/345315/products/{id}"
                url = f"https://api.glovoapp.com/v3/stores/383248/addresses/566074/products/{id}"
                headers = {
                    'authority': 'api.glovoapp.com',
                    'accept': 'application/json',
                    'accept-language': 'en-US,en;q=0.9',
                    'glovo-api-version': '14',
                    'glovo-app-development-state': 'Production',
                    'glovo-app-platform': 'web',
                    'glovo-app-type': 'customer',
                    'glovo-app-version': '7',
                    'glovo-client-info': 'web-customer-web/7 project:customer-web',
                    'glovo-device-urn': 'glv:device:26da6aee-f3e8-479a-acdf-f1b04af03ae2',
                    'glovo-dynamic-session-id': '64f9248c-e4f4-4808-afb2-7966e526e19a',
                    'glovo-language-code': 'en',
                    'glovo-location-city-code': 'SOF',
                    'glovo-request-id': '137fc4e4-a3bf-4360-89c8-26acc157e2cb',
                    'origin': 'https://glovoapp.com',
                    'referer': 'https://glovoapp.com/',
                    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }

                pdp_path = f"{db.PAGESAVE}/{id}.html"
                meta_dict = {"pdp_path":pdp_path}
                print('file:///' + pdp_path)
                if os.path.exists(pdp_path):
                    yield scrapy.Request(url='file:///' + pdp_path,cb_kwargs=meta_dict, callback=self.parse1,dont_filter=True)
                else:
                    # url = "https://api.glovoapp.com/v3/stores/195389/addresses/337911/products/19667120776"
                    yield scrapy.Request(url=url,headers=headers,cb_kwargs=meta_dict,callback=self.parse1,dont_filter=True)
        else:
            print("No any data found......")

    def parse1(self, response, **kwargs):
        item =Glovoapp_dataItem()
        pdp_path = kwargs['pdp_path']

        data = json.loads(response.text)

        Product_id = data['id']
        if Product_id:
            if not os.path.exists(pdp_path):
                db.pagesave(response, Product_id)
            else:
                print("Product Page Already Available")

            # open(self.pagesave + str(Product_id) + ".html", "wb").write(response.body)
            SKU_Name= data['name']

            #todo for Price_In_Local, Pack_Size_Local, Pack_Size
            item['Price_In_Local_USD'] = 'NA'
            Price_In_Local1 = data['price']
            Price_In_Local = "%.2f" % Price_In_Local1
            item['Price_In_Local'] = Price_In_Local

            Price_In_Local_USD1 = Price_In_Local1 * 0.5500
            Price_In_Local_USD = "%.2f" % Price_In_Local_USD1
            item['Price_In_Local_USD'] = Price_In_Local_USD

            item['Pack_Size_Local']=''
            string = data['name']
            pattern = r" \d+(\.\d+)?\s*(Мл|Л|cl|Lt|Litre|L)"

            match = re.search(pattern, string, flags=re.IGNORECASE)
            if match:
                Pack_Size_Local = match.group()
                item['Pack_Size_Local'] = Pack_Size_Local.strip()

                print(Pack_Size_Local)

                Pack_Sizes = Pack_Size_Local
                # numeric_value = int(''.join(filter(str.isdigit, Pack_Sizes)))
                numeric_value = re.findall('([-+]?(?:\d*\.*\d+))', Pack_Sizes, flags=re.IGNORECASE)
                if numeric_value:
                    numeric_value = float(numeric_value[0])
                unit = Pack_Sizes[-5:].strip()

                if unit.lower() == 'cl':
                    Pack_Size = numeric_value * 10

                elif unit.lower() == 'litre' or ('л' in unit.lower() and 'мл' not in unit.lower()):
                    Pack_Size = numeric_value * 1000

                else:
                    Pack_Size = numeric_value

                item['Pack_Size'] = Pack_Size


            # todo for Age_of_Whiskey
            item['Age_of_Whiskey'] = 'NA'
            string = data['name']
            # pattern = r"\d+\s*Год"
            pattern = r"\d+\s*YO"

            match = re.search(pattern, string)
            if match:
                years = match.group()
                item['Age_of_Whiskey'] = years .replace('YO','')
                print(years)

            #todo for image
            Image_Urls = data['imageUrl']

            abv=data['description']
            input_string = abv
            pattern = r'\b\d+%'
            matches = re.findall(pattern, input_string)
            if matches:
                percentage_value = matches[0]
                item['ABV'] =percentage_value

                print("Percentage value:", percentage_value)
            else:
                item['ABV'] = "NA"
                print("No percentage value found.")

            current_date = datetime.now()
            scrape_date = current_date.strftime("%d_%m_%Y")

            item['Platform_Name'] = "Glovoap-Billa"
            item['Platform_URL'] = "https://glovoapp.com/bg/en/sofia/billa-sof1/?content=napitki-sc.373843599%2Fvisokoalkoholni-napitki-c.2162070928"
            item['Product_id'] = Product_id
            item['Category'] = "NA"
            item['Sub_Category'] = "NA"
            item['Sector'] ="NA"
            item['Sub_Sector'] = "NA"
            item['SKU_Name'] = SKU_Name
            item['Manufacturer'] = "NA"
            item['Brand'] ="NA"
            item['Sub_Brand'] = "NA"
            item['Origin']= "NA"
            item['Major_Region'] = "Europe"
            item['Country'] = "Bulgaria"
            item['Market_Clusters'] = "EMEA Develop"
            # item['Pack_Size_Local']= "NA"
            # item['Pack_Size']= "NA"
            item['Price_In_Local']= Price_In_Local
            item['Price_In_Local_USD']= Price_In_Local_USD
            item['Type_of_Promo']= "NA"
            item['Promo_Price_Local']= "NA"
            item['Promo_Price_USD']= "NA"
            item['Price_per_unit_Local']= "NA"
            item['Price_per_unit_USD']= "NA"
            item['Price_per_unit_USD_New']= "NA"
            # item['Age_of_Whiskey']= "NA"
            item['Country_of_Origin']= "NA"
            item['Distillery']= "NA"
            item['Pack_type']= "NA"
            item['Tasting_Notes']= "NA"
            item['Image_Urls']= Image_Urls
            item['scrape_date'] = scrape_date
            item['Standard_Currency'] = "USD"

            # todo for Pack_Size_New, Price_Range
            a = ['NA', '704 ml', '1230 ml', '250 ml', '14.8 cl', '1600 ml', '2150 ml', '2100 ml', '1450 ml', '1980 ml',
                 '200 ml', '650 ml', '950 ml', '2.25 l', '2250 ml', '1780 ml', '150 ml', '1580 ml', '50 ml', '1.125 l',
                 '1300 ml', '1050 ml', '550 ml', '3500 ml', '1120 ml', '1280 ml', '2450 ml', '701 ml', '70 cl',
                 '900 ml', '846 ml', '2500 ml', '1350 ml', '2750 ml', '1.75 l', '2200 ml', '2800 ml', '1250 ml',
                 '800 ml', '1775 ml', '3000 ml', '10 cl', '3000 l', '5 cl', '330 ml', '440 ml', '568 ml', '125 ml',
                 '25 cl', '275 ml', '20 cl', '180 ml', '33 cl', '250ml', '125ml', '200ml', '100ml', '330ml', '720ml',
                 '20cl', '2640 ml', '5cl', '4x250', '10x250', '4x5cl', '4.5 l', '1750ml', '375ml', '5250ml', '640ml',
                 '50ml', '300ml', '725ml', '1125ml', '275ml', '1136ml', '4500ml', '30 ml', '355ml', '745ml', '400ml',
                 '3000ml', '100 ml', '6 x 375ml', '40mL', '1450ML', '360mL', '6 x 30mL', '180mL', '650mL', '230mL',
                 '6x50mL', '370mL', '8x50mL', '10330ML', '10 x 375mL', '175mL', '3 x 200mL', '12x40mL', '110mL',
                 '475mL', '2330mL', '24375ML', '130mL', '1420ML', '187mL', '220mL', '4x50mL', '1.125L', '900mL', '20mL',
                 '1.75L', '95mL', '1800mL', '3.4L', '800ML', '30mL', '3L', '60mL', '80mL', '3500mL', '2.1L', '4x 200mL',
                 '550mL', '3x50ml', '1.125 Litre', '440ml', '270ml', '660ml', '1lt', '1l', '510ml', '320ml', '120ml',
                 '140ml', '345ml', '300 l', '225 l', '72 cl', '14.6 cl', '12 cl', '2 cl', '375 ml', '3.5 cl', '4 cl',
                 '1.75 cl', '500 l', '215 l', '130 l', '1 cl', '47.3 cl', '255 l', '115 l', '290 l', '400 l', '125 l',
                 '7.5 cl', '6 cl', '1707 l', '210 l', '44 cl', '37.5 cl', '67 cl', '30 cl', '71ml', '0.200l', '660 ml',
                 '300 ml', '7.1 cl', '620 ml', '355 ml', '1100 ml', '1320 ml', '510 ml', '201 ml', '1.0 cl', '3 l',
                 '300 cl', '75 l', '0.2 l', '100 cl', '50.00 ml', '180.00 ml', '720.00 ml', '2100.00 ml', '4000.00 ml',
                 '2760.00 ml', '300.00 ml', '1800.00 ml', '100.00 ml', '360.00 ml', '200.00 ml', '660.00 ml',
                 '75.00 ml', '475.00 ml', '248.00 ml', '1.8L', '2200ML', '4L', '1920ML', '1440ML', '8 cl', '23 cl',
                 '1738 cl', '40 ml', '770 ml', '20 ml', '275ml x 6', '275ml x 24', '1.75 Litros', '175 Centilitros',
                 '60 Centilitros', '17.5 Centilitros', '35.50 cl', '33 Centilitros', '3 Litros', '20 Centilitros',
                 '682.8 g', '187 g', '250 Mililitros', '25 Centilitros']
            item['Pack_Size_New'] = 'NA'
            if item['Pack_Size_Local'] in a:
                item['Pack_Size_New'] = 'Others'

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
            yield item

    def closed(self, reason):
        """This method is called when the spider finishes."""
        print("Spider closed, reason:", reason)
        file_generation()  # Call your file generation function here

if __name__ == '__main__':
    execute("scrapy crawl link".split())
