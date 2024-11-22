import json
import os
import re
from datetime import datetime
import pymysql
import scrapy
from scrapy.cmdline import execute
import metrobg.db_config as db
from metrobg.items import MetrobgdataItem
from metrobg.EXPORT import file_generation

class DataexpractSpider(scrapy.Spider):
    name = "dataexpract"
    allowed_domains = ["shop.metro.bg"]
    # start_urls = ["https://www.google.com"]

    def __init__(self, name=None, start=0, end=0, **kwargs):
        # todo database connction & pagesave
        super().__init__(name, **kwargs)
        # DATABASE SPECIFIC VALUES
        self.start = int(start)
        self.end = int(end)
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password)
        self.cursor = self.con.cursor()
        self.PAGESAVE = db.PAGESAVE
        if not os.path.exists(self.PAGESAVE):
            os.makedirs(self.PAGESAVE)
    def start_requests(self):
        # todo fetching links from database and requesting that link
        query = f"select * FROM {db.db_links_table} where status='Pending' and id between {self.start} and {self.end} "
        self.cursor.execute(query)
        query_results = self.cursor.fetchall()
        self.logger.info(f"\n\n\nTotal Results ...{len(query_results)}\n\n\n", )
        for query_result in query_results:
            id = query_result[0]
            product_id = query_result[2]
            type = query_result[4]

            cookies = {
                'selectedLocale_BG': 'bg-BG',
                'BIGipServerbetty.metrosystems.net-80': '!Yhjp6q9ActWxkuyoXzlBiSJSvgTeQjk2gL2C2PsVuYNXPJkFd18wBMNpZtuZ+8ZvshSaqAiROGnCB0s=',
                'anonymousUserId': '47DF7409-BD94-4F24-ADCF-4F82DDD8BF7A',
                'allowedCookieCategories': 'necessary%7Cfunctional%7Cperformance%7Cpromotional%7CUncategorized',
                '_gcl_au': '1.1.1068008086.1698903689',
                'abGroups': '{%22CI_ARTICLE_BANNER%22:%22B%22%2C%22SD_NEW_SUGGESTIONS%22:%22B%22%2C%22QUICKENTRY_SEARCH_BACKEND%22:%22A%22%2C%22CI_USE_SEARCH_INSTEAD_OF_SIMPLESEARCH%22:%22B%22}',
                'local_ga': 'GA1.1.381332746.1698903690',
                'tsession': '{%22sessionId%22:%2235D6940E-A4BA-41B0-A7F8-F2D060002FBB%22%2C%22timestamp%22:1698903689000}',
                '_ga': 'GA1.2.381332746.1698903690',
                '_gid': 'GA1.2.136727509.1698903691',
                '_fbp': 'fb.1.1698905426735.1695817676',
                'UserSettings': 'SelectedStore=49880bc6-d87a-4291-9fbe-80ee87f401e7',
                '_dd_s': 'rum=0&expire=1698908000119',
                'local_ga_4N83E9ZK99': 'GS1.1.1698903689.1.1.1698907100.48.0.0',
            }

            headers = {
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'CallTreeId': 'C3BFDE44-45B6-4BD6-B279-0B243A23DA3F||BTEV-221EF565-1C64-48DD-829C-E8DE0A9B241D',
                'Connection': 'keep-alive',
                # 'Cookie': 'selectedLocale_BG=bg-BG; BIGipServerbetty.metrosystems.net-80=!Yhjp6q9ActWxkuyoXzlBiSJSvgTeQjk2gL2C2PsVuYNXPJkFd18wBMNpZtuZ+8ZvshSaqAiROGnCB0s=; anonymousUserId=47DF7409-BD94-4F24-ADCF-4F82DDD8BF7A; allowedCookieCategories=necessary%7Cfunctional%7Cperformance%7Cpromotional%7CUncategorized; _gcl_au=1.1.1068008086.1698903689; abGroups={%22CI_ARTICLE_BANNER%22:%22B%22%2C%22SD_NEW_SUGGESTIONS%22:%22B%22%2C%22QUICKENTRY_SEARCH_BACKEND%22:%22A%22%2C%22CI_USE_SEARCH_INSTEAD_OF_SIMPLESEARCH%22:%22B%22}; local_ga=GA1.1.381332746.1698903690; tsession={%22sessionId%22:%2235D6940E-A4BA-41B0-A7F8-F2D060002FBB%22%2C%22timestamp%22:1698903689000}; _ga=GA1.2.381332746.1698903690; _gid=GA1.2.136727509.1698903691; _fbp=fb.1.1698905426735.1695817676; UserSettings=SelectedStore=49880bc6-d87a-4291-9fbe-80ee87f401e7; _dd_s=rum=0&expire=1698908000119; local_ga_4N83E9ZK99=GS1.1.1698903689.1.1.1698907100.48.0.0',
                'Referer': 'https://shop.metro.bg/shop/pv/BTY-X271801/0032/0021/500%D0%BC%D0%BB-%D0%A1%D0%B0%D0%B9%D0%B4%D0%B5%D1%80-%D0%91%D0%BE%D1%80%D0%BE%D0%B2%D0%B8%D0%BD%D0%BA%D0%B0-%D0%A1%D0%B0%D0%BC%D1%8A%D1%80%D1%81%D0%B1%D0%B8-Somersby-%D0%9A%D0%B5%D0%BD',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
                'X-Event-Id': 'dd3e7980-9080-4e33-a64a-ab84caeaba2d',
                'X-Requested-With': 'XMLHttpRequest',
                'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
            }
            pdp_path = f"{db.PAGESAVE}/{product_id}.html"
            meta_dict = {
                "Id": id,
                "product_id": product_id,
                "type": type,
                "pdp_path": pdp_path
            }
            if os.path.exists(pdp_path):
                print('file:///' + pdp_path)
                yield scrapy.Request(url='file:///' + pdp_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
            else:
                final_url = f'https://shop.metro.bg/evaluate.article.v1/betty-articles?ids={product_id}&country=BG&locale=bg-BG&storeIds=00010&details=true&__t=1698906550441'                # final_url = 'https://shop.metro.bg/evaluate.article.v1/betty-articles?ids=BTY-X170362&country=BG&locale=bg-BG&storeIds=00010&details=true&__t=1693862122777'
                yield scrapy.Request(url=final_url,headers=headers,cookies=cookies,dont_filter=True,cb_kwargs=meta_dict)
    def parse(self, response,**kwargs):
        item = MetrobgdataItem()
        item['Id'] = kwargs['Id']
        item['type'] = kwargs['type']
        item['Pack_Size_Local'] = ''
        product_id = kwargs['product_id']
        pdp_path = kwargs['pdp_path']
        if product_id:
            if not os.path.exists(pdp_path):
                # todo pagesave
                db.pagesave(response, product_id)
            else:
                print("File Already Available...")

            data = json.loads(response.text)
            # todo updating links table status
            if not data['result']:
                self.cursor.execute(f'update {db.db_links_table} set status="404" where Id="{item["Id"]}"')
                self.con.commit()
                print("NA")
                return None
            else:
                data=data['result'][f'{product_id}']['variants']['0032']

            # data = json.loads(response.text)['result']['BTY-X170362']['variants']['0032']

            # todo for ABV, SKU_Name, Pack_Size_Local
            features = data['bundles']['0021']['details']['features']
            unit = None
            for i in features:
                if i['featureType'] == 'countryOfOriginStatement':
                    item['Country_of_Origin'] = i['leafs'][0]['label']
                if i['featureType'] == 'percentageOfAlcoholByVolume':
                    abv = i['value']
                    if abv:
                        fin_abv = re.findall('(\d+\.?\d*)', abv)
                        if fin_abv:
                            item['ABV'] = fin_abv[0].replace(',','.').strip()
                    # item['ABV'] = ().replace('% vol.','').replace('%vol.','').replace('%','').replace(',','.').replace(' градуса','.')
                if i['featureType'] == 'regulatedProductName':
                    item['SKU_Name'] = i['leafs'][0]['label']
                if i['featureType'] == 'netContent':
                    unit = i['unit']
                    item['Pack_Size_Local'] = i['value']

            if item.get('SKU_Name') == None:
                item['SKU_Name'] = data['description']


            item['Platform_Name'] = 'Metro'
            item['Platform_URL'] = f'https://shop.metro.bg/shop/pv/{product_id}/0032/0021'
            item['Product_id'] = product_id
            item['Category'] = data['group']['mainGroupName']
            item['Brand'] = data['bundles']['0021']['brandName']
            item['Major_Region'] = 'Europe'
            item['country'] = 'Bulgaria'
            item['Market_Clusters'] = 'EMEA Develop'
            # try:
            #     pack_size = data['bundles']['0021']['contentData']['netContentVolume']
            #     item['Pack_Size'] = pack_size['value'].replace('℮ ', '').strip()
            #     if 'мл' in item['Pack_Size_Local']:

            #     else:
            #         web_pack = item['Pack_Size_Local'].replace('л', '').replace('℮ ', '').strip()
            #         item['Pack_Size'] = 1000 * float(web_pack)
            #     item['SKU_Name'] = item['SKU_Name'].replace(f"{item['Pack_Size_Local']}",'').replace('.', '').strip()
            #
            #     if unit == 'л':
            #         item['SKU_Name'] = item['SKU_Name'b.kkb].replace(f"{item['Pack_Size_Local']}", '').replace(f"{unit}",'').replace('.','').strip()
            #         web_pack = item['Pack_Size_Local'].replace('л', '').strip()
            #         item['Pack_Size'] = 1000 * float(web_pack)
            #     else:
            #         item['Pack_Size'] = item['Pack_Size_Local'].replace('мл', '').replace('.', '').strip()
            #
            # except Exception as e:
            #     print(e)

            # todo for Pack_Size
            try:
                if len(item['Pack_Size_Local']) >= 3:
                    item['Pack_Size'] = item['Pack_Size_Local']
                if 'мл' in item['Pack_Size_Local']:
                    item['Pack_Size'] = item['Pack_Size_Local'].replace('мл', '').replace('℮ ', '').replace('.', '').strip()
                    if len(item['Pack_Size']) <= 2:
                        str(int(100 * float(item['Pack_Size']))).replace(',', '.')
                elif unit == 'мл':
                    item['Pack_Size'] = item['Pack_Size_Local'].replace('мл', '').replace('℮ ', '').replace('.', '').strip()
                    if len(item['Pack_Size']) <= 2:
                        str(int(100 * float(item['Pack_Size']))).replace(',', '.')
                elif ' л.' in item['Pack_Size_Local']:
                    web_pack = item['Pack_Size_Local'].replace(' л.', '').replace('℮ ', '').replace(',', '.').strip()
                    item['Pack_Size'] = str(int(1000 * float(web_pack))).replace('.', '').strip()
                else:
                    web_pack = item['Pack_Size_Local'].replace('л', '').replace('℮ ', '').replace(',', '.').strip()
                    item['Pack_Size'] = str(int(1000 * float(web_pack))).replace('.', '').strip()
                # else:
                #     item['Pack_Size'] = item['Pack_Size_Local'].replace('мл', '').replace('℮ ', '').replace('.', '').replace(',', '.').strip()
                #     if len(item['Pack_Size']) <= 2:
                #         item['Pack_Size'] = str(int(100 * float(item['Pack_Size']))).replace(',', '.')
            except Exception as e:
                print(e)

            #todo for pack_size
            string = item['SKU_Name']
            # pattern = r" \d+(\.\d+)?\s*(Мл|Л|л|cl|Lt|Litre|L)"
            pattern = r'([-+]?(?:\d*\.*\d+))\s*(Мл|Л|л|cl|Lt|Litre|ml|L)'

            # todo for Pack_Size_Local,Pack_Size
            item['Pack_Size_Local'] = 'NA'
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

            # try:
            #     item['SKU_Name'] = item['SKU_Name'].replace(f"{item['Pack_Size_Local']}",'').replace(f"{item['Pack_Size']}",'').replace(' л.','').replace('л.','').replace('л','').replace('мл','').replace('0.7 ','').replace('1 ','').replace('1,5 ','').replace('.','').strip()
            # except:...
            item['Price_In_Local_USD'] = 'NA'
            item['Price_In_Local'] = 'NA'
            item['Promo_Price_Local'] = 'NA'
            item['Promo_Price_USD'] = 'NA'

            # todo for price_in_local,Promo_Price_Local
            try:
                price = data['bundles']['0021']['stores']['00010']['sellingPriceInfo']['finalPrice']
            except:
                price = ''
            try:
                pprice = \
                data['bundles']['0021']['stores']['00010']['possibleDeliveryModes']['STORE']['possibleFulfillmentTypes'][
                    'STORE']['sellingPriceInfo']['grossStrikeThrough']
            except:
                pprice = ''
            if price and pprice:
                item['Price_In_Local'] = pprice
                item['Price_In_Local_USD'] = round(pprice * 0.55,2)
                item['Promo_Price_Local'] = price
                item['Promo_Price_USD'] = round(price * 0.55,2)
            else:
                item['Price_In_Local'] = price
                item['Price_In_Local_USD'] = round(price * 0.55,2)

            # todo for Age_of_Whisky, Pack_type, Image_Urls
            for_age = re.findall('(\d+\.?\d*)\s*[Year|yo|YO|year|yr|годишно|]', item['SKU_Name'], flags=re.IGNORECASE)
            if for_age:
                age_val = for_age[0]
                item["Age_of_Whisky"] = age_val
            else:
                item['Age_of_Whisky'] = ''

            if not item["Age_of_Whisky"]:
                for_age = re.findall('(\d+\.?\d*)\s*-year-old', item['SKU_Name'], flags=re.IGNORECASE)
                if for_age:
                    age_val = for_age[0]
                    item["Age_of_Whisky"] = age_val

            item['Pack_type'] = 'Bottle'
            images = data['bundles']['0021']['details']['media']['images']
            if images:
                item['Image_Urls'] = images[0]['url'].replace('{?w,h,mode}','')
            current_date = datetime.now()
            item['scrape_date'] = current_date.strftime("%d_%m_%Y")

            # todo Pack_Size_New, Price_Range
            a = ['NA', '704 мл', '1230 мл', '250 мл', '14.8 cl', '1600 мл', '2150 мл', '2100 мл', '1450 мл', '1980 мл',
                 '200 мл', '650 мл', '950 мл', '2.25л', '2250 мл', '1780 мл', '150 мл', '1580 мл', '50 мл', '1.125л',
                 '1300 мл', '1050 мл', '550 мл', '3500 мл', '1120 мл', '1280 мл', '2450 мл', '701 мл', '70 cl',
                 '900 мл', '846 мл', '2500 мл', '1350 мл', '2750 мл', '1.75л', '2200 мл', '2800 мл', '1250 мл',
                 '800 мл', '1775 мл', '3000 мл', '10 cl', '3000л', '5 cl', '330 мл', '440 мл', '568 мл', '125 мл',
                 '25 cl', '275 мл', '20 cl', '180 мл', '33 cl', '250мл', '125мл', '200мл', '100мл', '330мл', '720мл',
                 '20cl', '2640 мл', '5cl', '4x250', '10x250', '4x5cl', '4.5л', '1750мл', '375мл', '5250мл', '640мл',
                 '50мл', '300мл', '725мл', '1125мл', '275мл', '1136мл', '4500мл', '30 мл', '355мл', '745мл', '400мл',
                 '3000мл', '100 мл', '6 x 375мл', '40мл', '1450мл', '360мл', '6 x 30мл', '180мл', '650мл', '230мл',
                 '6x50мл', '370мл', '8x50мл', '10330мл', '10 x 375мл', '175мл', '3 x 200мл', '12x40мл', '110мл',
                 '475мл', '2330мл', '24375мл', '130мл', '1420мл', '187мл', '220мл', '4x50мл', '1.125L', '900мл', '20мл',
                 '1.75L', '95мл', '1800мл', '3.4L', '800мл', '30мл', '3L', '60мл', '80мл', '3500мл', '2.1L', '4x 200мл',
                 '550мл', '3x50мл', '1.125лitre', '440мл', '270мл', '660мл', '1lt', '1l', '510мл', '320мл', '120мл',
                 '140мл', '345мл', '300л', '225л', '72 cl', '14.6 cl', '12 cl', '2 cl', '375 мл', '3.5 cl', '4 cl',
                 '1.75 cl', '500л', '215л', '130л', '1 cl', '47.3 cl', '255л', '115л', '290л', '400л', '125л',
                 '7.5 cl', '6 cl', '1707л', '210л', '44 cl', '37.5 cl', '67 cl', '30 cl', '71мл', '0.200l', '660 мл',
                 '300 мл', '7.1 cl', '620 мл', '355 мл', '1100 мл', '1320 мл', '510 мл', '201 мл', '1.0 cl', '3л',
                 '300 cl', '75л', '0.2л', '100 cl', '50.00 мл', '180.00 мл', '720.00 мл', '2100.00 мл', '4000.00 мл',
                 '2760.00 мл', '300.00 мл', '1800.00 мл', '100.00 мл', '360.00 мл', '200.00 мл', '660.00 мл',
                 '75.00 мл', '475.00 мл', '248.00 мл', '1.8L', '2200мл', '4L', '1920мл', '1440мл', '8 cl', '23 cl',
                 '1738 cl', '40 мл', '770 мл', '20 мл', '275мл x 6', '275мл x 24', '1.75лitros', '175 Centilitros',
                 '60 Centilitros', '17.5 Centilitros', '35.50 cl', '33 Centilitros', '3лitros', '20 Centilitros',
                 '682.8 g', '187 g', '250 Mililitros', '25 Centilitros']
            item['Pack_Size_New'] = 'NA'
            if item['Pack_Size_Local'] in a:
                item['Pack_Size_New'] = 'Others'

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
        # check_pending = f"select * FROM {db.db_links_table} where status='Pending'"
        # self.cursor.execute(check_pending)
        # query_results = self.cursor.fetchall()
        # if len(query_results) == 0:
        #     """This method is called when the spider finishes."""
        #     print("Spider closed, reason:", reason)
        #     file_generation()  # Call your file generation function here
        # else:
        #     print(f"File Not Generate Due to {len(query_results)} record still pending.")

if __name__ == '__main__':
    execute('scrapy crawl dataexpract -a start=1 -a end=100000000000000'.split())
    # execute('scrapy crawl dataexpract -a start=1 -a end=1'.split())