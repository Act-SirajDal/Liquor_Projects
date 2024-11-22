from datetime import datetime
import os
import re
import pymysql
import pymysql.cursors
import scrapy
# from scraper_api import ScraperAPIClient
from scrapy.cmdline import execute
from scrapy.utils.response import open_in_browser
from translate import Translator
from amazon_jp.items import *
import amazon_jp.db_config as db
from amazon_jp.EXPORT import file_generation

def translate_polish_to_english(text):
    translator = Translator(to_lang="en", from_lang="ja")
    translation = translator.translate(text)
    return translation

class DataSpider(scrapy.Spider):
    name = 'data_extractor'
    handle_httpstatus_list = [404]

    
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


    def __init__(self, start=0, end=0,):
        self.con = pymysql.connect(host=db.db_host,user=db.db_user,password=db.db_password, database=db.db_name)
        self.cursor = self.con.cursor(pymysql.cursors.DictCursor)
        self.start = int(start)
        self.end = int(end)
        self.PAGESAVE = db.PAGESAVE
        self.proxy = 'http://9dbe950ef6284a5da9e7749db9f7cbd1:@api.zyte.com:8011'

    def start_requests(self):
        self.cursor.execute(f"""SELECT * FROM {db.db_links_table} WHERE status = 'Pending' AND Id BETWEEN {self.start} and {self.end}""")
        links = self.cursor.fetchall()
        print(f'len of links.....{len(links)}')

        cookies = {
            'session-id': '357-6302843-7857327',
            'session-id-time': '2082787201l',
            'x-amz-captcha-1': '1703613727038979',
            'x-amz-captcha-2': '6m7iDJ7PZ660Cbs/XI6PqQ==',
            'ubid-acbjp': '357-4366387-9940562',
            'i18n-prefs': 'JPY',
            'sp-cdn': '"L5Z9:IT"',
            'lc-acbjp': 'ja_JP',
            'session-token': '"bdXEmky0dUbBDdIpDrjCJjy8PNjgRicQwdbMlzLxYonm30YI/HZZ+JYhgLi4GoGnKhHtisGGXQTRZE/7RhNqwk714M4/d8QS+/XRR05fW5fnuhEfuTLVrEoy/8zFaI2+bNoeT4Ugg/uVPo9hIw1lc1MrfU+eK/keJeVGMky83mbZmvBvYhYwSgi6K0zZelb6OHXXNN3QGSGQa196mE84p1i5rQl6ZPECtLLCeszVKZLbcKbyMTNYAZoDi9kSZdkrRJ1DkDZz5+/ZPu/E913J7aYe0P+7d2sp4K+B7WZq5u2KEsx7FYj1kO3jSV/4YA9fqi0JfeqjfKLmFFHiMNfDAjIUH+shwIX3HWRSFee/6BM="',
            'csm-hit': 'tb:s-DP4HFQM4HSHQGZ3CNZHE|1709288800417&t:1709288801681&adb:adblk_no',
        }

        headers = {
            'authority': 'www.amazon.co.jp',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            # 'cookie': 'session-id=357-6302843-7857327; session-id-time=2082787201l; x-amz-captcha-1=1703613727038979; x-amz-captcha-2=6m7iDJ7PZ660Cbs/XI6PqQ==; ubid-acbjp=357-4366387-9940562; i18n-prefs=JPY; sp-cdn="L5Z9:IT"; lc-acbjp=ja_JP; session-token="bdXEmky0dUbBDdIpDrjCJjy8PNjgRicQwdbMlzLxYonm30YI/HZZ+JYhgLi4GoGnKhHtisGGXQTRZE/7RhNqwk714M4/d8QS+/XRR05fW5fnuhEfuTLVrEoy/8zFaI2+bNoeT4Ugg/uVPo9hIw1lc1MrfU+eK/keJeVGMky83mbZmvBvYhYwSgi6K0zZelb6OHXXNN3QGSGQa196mE84p1i5rQl6ZPECtLLCeszVKZLbcKbyMTNYAZoDi9kSZdkrRJ1DkDZz5+/ZPu/E913J7aYe0P+7d2sp4K+B7WZq5u2KEsx7FYj1kO3jSV/4YA9fqi0JfeqjfKLmFFHiMNfDAjIUH+shwIX3HWRSFee/6BM="; csm-hit=tb:s-DP4HFQM4HSHQGZ3CNZHE|1709288800417&t:1709288801681&adb:adblk_no',
            'device-memory': '8',
            'downlink': '1.3',
            'dpr': '1.25',
            'ect': '3g',
            'rtt': '350',
            'sec-ch-device-memory': '8',
            'sec-ch-dpr': '1.25',
            'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"15.0.0"',
            'sec-ch-viewport-width': '1088',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'viewport-width': '1088',
        }

        for link in links:
            url = link['url']
            product_id = link['product_id']
            Id = link['Id']
            pdp_path = f"{db.PAGESAVE}/{product_id}.html"
            meta_dict = {
                "url": url,
                "Id": Id,
                "product_id": product_id,
                "pdp_path": pdp_path
            }
            if os.path.exists(pdp_path):
                print('file:///' + pdp_path)
                yield scrapy.Request(url='file:///' + pdp_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
            else:
                yield scrapy.Request(url=url,headers=headers,cookies=cookies,dont_filter=True,cb_kwargs=meta_dict,meta={'proxy': self.proxy})

    def parse(self, response,**kwargs):
        item = AmazonJpData()
        exchange_rate = 0.0068
        item['Id'] = kwargs['Id']
        pdp_path = kwargs['pdp_path']
        product_id = kwargs['product_id']
        if response.status == 404:
            self.cursor.execute(f"""UPDATE {db.db_links_table} SET STATUS='404' WHERE Id='{item["Id"]}'""")
            self.con.commit()
            print("404")
            return None

        if response.xpath('//h4[contains(text(),"Enter the characters you see below")]'):
            self.cursor.execute(f"""UPDATE {db.db_links_table} SET STATUS='Block' WHERE product_id='{item['Product_id']}'""")
            self.con.commit()
            return None

        SKU_Name = response.xpath('//h1[@id="title"]/span/text()').get()
        if not SKU_Name:
            print("BLOCKED")
            return None
        else:
            if not os.path.exists(pdp_path):
                # todo pagesave
                db.pagesave(response, product_id)
            else:
                print("File Already Available...")

            item['Platform_Name'] = 'Amazon'
            item['Platform_URL'] = kwargs['url']
            item['Product_id'] = kwargs['product_id']
            item['scrape_date'] = datetime.today().strftime("%d_%m_%Y")
            item['Country'] = 'Japan'
            item['Category'] = ''
            item['Manufacturer'] = ''
            item['Sub_Category'] = 'NA'
            item['Brand'] = "NA"
            item['ABV'] = ""
            item['Pack_Size_Local'] = ''
            item['Pack_Size'] = ''
            item['Sector'] = 'NA'
            item['Sub_Sector'] = 'NA'
            item['Promo_Price_USD'] = 'NA'
            item['Promo_Price_Local'] = 'NA'
            item['Price_In_Local'] = "NA"
            item['Price_In_Local_USD'] = "NA"
            item['Price_per_unit_Local'] = ''
            item['Price_per_unit_USD'] = ''
            item['Country_of_Origin'] = ''
            item['Pack_type'] = ''
            item['Age_of_Whisky'] = ''
            item['Sub_Brand'] = ''
            item['Origin'] = 'NA'
            item['Major_Region'] = 'Asia Pacific'
            item['Market_Clusters'] = 'APAC Develop'
            item['Type_of_Promo'] = 'NA'
            item['Distillery'] = 'NA'
            item['Tasting_Notes'] = 'NA'
            item['Standard_Currency'] = 'USD'
            item['SKU_Name'] = SKU_Name.strip()

            # abv = re.findall(r'([0-9]*[\,|\.]?[0-9])\s*%[vol|abv]', item['SKU_Name'], flags=re.IGNORECASE)
            abv = re.findall(r'([-+]?(?:\d*[\,|\.]*\d+))%\s*vol', item['SKU_Name'], flags=re.IGNORECASE)
            if not abv:
                abv = re.findall(r'([-+]?(?:\d*[\,|\.]*\d+))%\s*abv', item['SKU_Name'], flags=re.IGNORECASE)
            if not abv:
                # abv = re.findall(r'ABV\s*([0-9]*[\,|\.]?[0-9])\s*%', item['SKU_Name'], flags=re.IGNORECASE)
                abv = re.findall(r'ABV\s*([-+]?(?:\d*[\,|\.]*\d+))\s*%', item['SKU_Name'], flags=re.IGNORECASE)
            if not abv:
                abv = re.findall(r'([-+]?(?:\d*[\,|\.]*\d+))\s*%', item['SKU_Name'], flags=re.IGNORECASE)

            if not abv:
                about_item2 = response.xpath('//div[@id="feature-bullets"]//li/span/text()').getall()
                if about_item2:
                    about_item = ''.join(about_item2)
                    abv = re.findall(r'([-+]?(?:\d*[\,|\.]*\d+))\s*%\s*ABV', about_item, flags=re.IGNORECASE)
                    if abv: item['ABV'] = abv[0]

            if abv:item['ABV'] = abv[0]

            age_whisky = re.findall(r'([0-9]*[\,|\.]?[0-9])\s*year', item['SKU_Name'], flags=re.IGNORECASE)
            if not age_whisky:
                age_whisky =re.findall(r'([0-9]*[\,|\.]?[0-9])\s*年', item['SKU_Name'], flags=re.IGNORECASE)
            if not age_whisky:
                age_whisky =re.findall(r'([0-9]*[\,|\.]?[0-9])\s*-年', item['SKU_Name'], flags=re.IGNORECASE)
            if not age_whisky:
                age_whisky = re.findall(r'([0-9]*[\,|\.]?[0-9])\s*-year', item['SKU_Name'], flags=re.IGNORECASE)
            if not age_whisky:
                age_whisky = re.findall(r'([0-9]*[\,|\.]?[0-9])\s*yo', item['SKU_Name'], flags=re.IGNORECASE)
            if not age_whisky:
                age_whisky = re.findall(r'([0-9]*[\,|\.]?[0-9])\s*よー', item['SKU_Name'], flags=re.IGNORECASE)
            if not age_whisky:
                age_whisky = re.findall(r'([0-9]*[\,|\.]?[0-9])\s*yr', item['SKU_Name'], flags=re.IGNORECASE)
            if age_whisky:
                item['Age_of_Whisky'] = age_whisky[0]

            # ABOUT ITEM
            about_item1 = response.xpath('//table[@class="a-normal a-spacing-micro"]//tr')
            if about_item1:
                dicts = {}
                for about in about_item1:
                    key = about.xpath('./td[@class="a-span3"]/span/text()').get()
                    value = about.xpath('./td[@class="a-span9"]/span/text()').get()
                    dicts[key] = value
                    if key == 'Alcohol type':
                        item['Category_1'] = value
                    if key == 'Brand':
                        item['Brand'] = value
                    if key.lower() == 'unit count':
                        pack_size = value
                        if 'milli' in pack_size or 'ミリリットル' in pack_size:
                            item['Pack_Size'] = "%.2f" % float(re.findall('([-+]?(?:\d*[\,|\.]*\d+))', pack_size)[0])
                            item['Pack_Size_Local'] = f"{item['Pack_Size']} ml"
                        elif 'centi' in pack_size or 'cl' in pack_size:
                            item['Pack_Size'] = "%.2f" % (float(''.join(re.findall('([-+]?(?:\d*[\,|\.]*\d+))', pack_size))) * 10)
                            item['Pack_Size_Local'] = f"{item['Pack_Size']} cl"
                        elif ' l' in pack_size:
                            item['Pack_Size'] = "%.2f" % (float(''.join(re.findall('([-+]?(?:\d*[\,|\.]*\d+))', pack_size))) * 1000)
                            item['Pack_Size_Local'] = f"{item['Pack_Size']} l"

            # CATEGORY
            category = response.xpath('//*[@class="a-unordered-list a-horizontal a-size-small"]/li/span/a/text()').getall()
            if category:
                cat = []
                count = 0
                for i in category:
                    if i:
                        cate = i.replace('\n', '').strip()
                        cat.append(cate)
                        if len(cat) == 3:
                            item['Category_2'] = cat[-2]
                            item['Category'] = cat[-1]
                        if len(cat) == 4:
                            item['Category_2'] = cat[-3]
                            item['Category'] = cat[-2]
                            item['Sub_Category'] = cat[-1]
            else:
                item['Category'] = 'NA'
                item['Sub_Category'] = 'NA'

            # PRODUCT INFO.
            product_info = response.xpath('//*[@id="productDetails_techSpec_section_1"]//tr')
            dicts ={}
            if product_info:
                product_info_keys = []
                for info in product_info:
                    key = info.xpath('.//th/text()').get().strip()
                    value = info.xpath('.//td/text()').get()
                    value = value.replace('\n', '').strip()
                    dicts[key] = value
                    if item['Brand'] == "NA":
                        if key == 'Brand':
                            item['Brand'] = value
                    if key == 'Volume' or key == 'Liquid Volume' or key=='Internal capacity' or key.lower() == 'units':
                        if not item['Pack_Size']:
                            pack_size = dicts[key].lower()
                            if 'milli' in pack_size:
                                item['Pack_Size'] = "%.2f" % float(re.findall('([-+]?(?:\d*[\,|\.]*\d+))', pack_size,flags=re.IGNORECASE)[0])
                                item['Pack_Size_Local'] = f"{item['Pack_Size']} ml"
                            elif 'centi' in pack_size or 'cl' in pack_size:
                                pack_size = ''.join(re.findall('([-+]?(?:\d*[\,|\.]*\d+))', pack_size))
                                item['Pack_Size'] = "%.2f" % (float(pack_size) * 10)
                                item['Pack_Size_Local'] = f"{pack_size} cl"
                            else:
                                pack_size = ''.join(re.findall('([-+]?(?:\d*[\,|\.]*\d+))', pack_size))
                                item['Pack_Size'] = "%.2f" % (float(pack_size) * 1000)
                                item['Pack_Size_Local'] = f"{pack_size} l"

                    elif key == 'Manufacturer':
                        item['Manufacturer'] = value

                    elif key in ['Country Produced In', 'Country of origin']:
                        item['Country_of_Origin'] = value
                    elif key == 'Region Produced In':
                        item['Origin'] = value
                    elif key == 'Package Information':
                        try:
                            if '瓶' in value:item['Pack_type'] = 'Bottle'
                            else:item['Pack_type'] = re.search(r'\b(jar|bottle|bottles|can|cans|pack|case|box)\b',value, re.IGNORECASE).group()
                        except:pass
                    elif key == 'Alcohol Content':
                        item['ABV'] = re.findall('([-+]?(?:\d*[\,|\.]*\d+))', value)[0]


                if not item['Pack_Size']:
                    # pack_size = "%.2f" % float(''.join(re.findall('([0-9]*\.?[0-9])\s*ml', item['SKU_Name'])))
                    pack_size = re.findall('([-+]?(?:\d*[\,|\.]*\d+))\s*ml', item['SKU_Name'], flags=re.IGNORECASE)
                    if pack_size:
                        item['Pack_Size'] = "%.2f" % float(pack_size[0].replace(',',''))
                        item['Pack_Size_Local'] = f"{item['Pack_Size']} ml"
                    if not pack_size:
                        pack_size = re.findall('([-+]?(?:\d*[\,|\.]*\d+))\s*cl', item['SKU_Name'], flags=re.IGNORECASE)
                        if pack_size:
                            pack_size = pack_size[0]
                            item['Pack_Size'] = "%.2f" % (float(pack_size) * 10)
                            item['Pack_Size_Local'] = f"{pack_size} cl"
                    if not pack_size:
                        pack_size = re.findall('([-+]?(?:\d*[\,|\.]*\d+))\s*l ', item['SKU_Name'], flags=re.IGNORECASE)
                        if pack_size:
                            pack_size = pack_size[0]
                            item['Pack_Size'] = "%.2f" % (float(pack_size) * 1000)
                            item['Pack_Size_Local'] = f"{pack_size} l"

            else:pass

            string = SKU_Name
            pack = db.pack_size(self, string.replace(',', '.').replace('+', ''))
            if pack:
                if pack[1]:
                    item['Pack_Size_Local'] = pack[1]
                    item['Pack_Size'] = db.pack_size(self, item['Pack_Size_Local'])[0]

                else:
                    item['Pack_Size_Local'] = pack[1]
                    item['Pack_Size'] = pack[0]
            # PRODUCT DETAILS
            overview = response.xpath('//div[@id="productOverview_feature_div"]//tr')
            details = response.xpath('//div[@id="detailBullets_feature_div"]//li')

            if overview or details:
                dicts = {}
                for i in overview:
                    key = i.xpath('.//td/span[@class="a-size-base a-text-bold"]/text()').get()
                    value = i.xpath('./td[@class="a-span9"]/span/text()').get()
                    dicts[key] = value

            prices = set({i.strip() for i in response.xpath('//td[contains(text(),"Price:")]/text()').getall() if i.strip()})

            if len(prices) == 1:
                price_without_discount = response.xpath('//td[contains(text(),"Price:")]//following-sibling::td/span[1]/span/text()').get()
                # re.findall('([-+]?(?:\d*[\,|\.]*\d+))\s*l', item['SKU_Name'], flags=re.IGNORECASE):
                try:
                    price_without_discount = float(re.findall('([-+]?(?:\d*[\,|\.]*\d+))',price_without_discount,flags=re.IGNORECASE)[0].replace(',',''))
                    item['Price_In_Local'] = "%.2f" % price_without_discount
                    item['Price_In_Local_USD'] = "%.2f" % (price_without_discount * exchange_rate)
                except:pass
            elif len(prices) == 2:
                price_list = []
                price_without_discount = response.xpath('//td[contains(text(),"Price:")]//following-sibling::td/span[1]/span[1]')
                for pric in price_without_discount:
                    price1 = pric.xpath('./text()').get('')
                    price_list.append(price1)
                if price_list:
                    price_max = max(price_list)
                    price_max = float(re.findall('([-+]?(?:\d*[\,|\.]*\d+))',price_max,flags=re.IGNORECASE)[0].replace(',',''))
                    price_min = min(price_list)
                    price_min = float(re.findall('([-+]?(?:\d*[\,|\.]*\d+))',price_min,flags=re.IGNORECASE)[0].replace(',',''))
                    item['Price_In_Local'] = "%.2f" % price_max
                    item['Price_In_Local_USD'] = "%.2f" % (price_max * exchange_rate)
                    item['Promo_Price_USD'] = "%.2f" % (price_min * exchange_rate)
                    item['Promo_Price_Local'] = "%.2f" % price_min
                if item['Price_In_Local_USD'] == item['Promo_Price_USD']:
                    item['Promo_Price_USD'] = 'NA'
                    item['Promo_Price_Local'] = 'NA'

            if item['Price_In_Local_USD'] == 'NA':

            # PRICE WITHOUT DISCOUNT
                price_without_discount1 = response.xpath('//*[contains(text(),"RRP:")]//following-sibling::*//span[@class="a-size-base a-color-secondary"]/text()').get('').strip()
                price_without_discount2 = response.xpath('//*[contains(text(),"Was:") or contains(text(),"RRP:") or contains(text(),"List Price")]//following-sibling::*[@data-a-size="s"]/span/text()').get('').strip()
                price_without_discount3 = response.xpath('//*[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]/span/text()').get('').strip()
                price_without_discount4 = response.xpath('//div[@id="corePriceDisplay_desktop_feature_div"]//div[contains(@class,"aok-align-center")]/span/span/text()').get('').strip()
                price_without_discount5 = response.xpath('//div[@id="corePriceDisplay_desktop_feature_div"]/div/span[contains(text(),"£")]/text()').get('')
                price_without_discount6 = response.xpath('//*[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]/span/span[@class="a-price-whole"]/text()').get()
                if price_without_discount4:
                    price_without_discount4 = re.findall('([-+]?(?:\d*[\,|\.]*\d+))',price_without_discount4,flags=re.IGNORECASE)
                    if price_without_discount4:
                        price_without_discount4 = float(price_without_discount4[0].replace(',',''))
                        item['Promo_Price_Local'] = "%.2f" %price_without_discount4
                        # item['Promo_Price_USD'] = round(float(price_without_discount4.replace('£',''))*exchange_rate,2)
                        item['Promo_Price_USD'] = "%.2f" % (price_without_discount4 * exchange_rate)
                if price_without_discount1:
                    price_without_discount = price_without_discount1
                elif price_without_discount2:
                    price_without_discount = price_without_discount2
                elif price_without_discount3:
                    price_without_discount = price_without_discount3
                elif price_without_discount5:
                    price_without_discount = price_without_discount5
                elif price_without_discount6:
                    price_without_discount = price_without_discount6
                else:
                    price_without_discount = ''
                if price_without_discount:
                    price_without_discount = float(re.findall('([-+]?(?:\d*[\,|\.]*\d+))',price_without_discount,flags=re.IGNORECASE)[0].replace(',',''))
                    item['Price_In_Local'] = "%.2f" % price_without_discount
                    item['Price_In_Local_USD'] = "%.2f" % (price_without_discount * exchange_rate)

                if item['Price_In_Local_USD'] == item['Promo_Price_USD']:
                    item['Promo_Price_USD'] = 'NA'
                    item['Promo_Price_Local'] = 'NA'

            # PRICE PER UNIT
            # price_per_unit1 = response.xpath('//*[@class="a-size-mini a-color-base aok-align-center pricePerUnit"]//text()').getall()
            price_per_unit1 = response.xpath('//*[@class="a-size-mini a-color-base aok-align-center pricePerUnit"]//*[contains(text(),"£")]/text()').get()
            if price_per_unit1:
                unit = ''
                for i in price_per_unit1:
                    if i not in unit:
                        unit = unit + i.replace('\n', '')
                price_per_unit = unit.strip().strip('(').strip(')')
            else:
                price_per_unit = ""
            price_per_unit2 = response.xpath('//td[contains(text(),"Price:")]//following-sibling::td//span[@data-a-size="b"]/parent::span//text()').getall()
            if price_per_unit2:
                unit = ''
                for i in price_per_unit2:
                    if i not in unit:
                        unit = unit + i.replace('\n', '')
                price_per_unit = unit.strip().strip('(').strip(')')
            price_per_unit3 = response.xpath('//td[contains(text(),"Price:")]//following-sibling::td//span[@id="sns-base-price"]/span/text()').getall()
            if price_per_unit3:
                unit = ''
                for i in price_per_unit3:
                    if i not in unit:
                        unit = unit + i.replace('\n', '')
                price_per_unit = unit.strip().strip('(').strip(')')
            price_per_unit4 = response.xpath('//div[@id="corePriceDisplay_desktop_feature_div"]//span[contains(text(),"l") or contains(text(),"/") or contains(text(),"(") or contains(text(),")")]//span/text()').get()
            if price_per_unit4:
                price_per_unit = f'{price_per_unit4}'
            if price_per_unit:
                price_per_unit = re.findall('([-+]?(?:\d*[\,|\.]*\d+))',price_per_unit,flags=re.IGNORECASE)
                if price_per_unit:
                    price_per_unit = float(price_per_unit[0].replace(',',''))
                    item['Price_per_unit_Local'] = "%.2f" % price_per_unit
                    item['Price_per_unit_USD'] = "%.2f" % (price_per_unit * exchange_rate)
            else:
                item['Price_per_unit_Local'] = "NA"
                item['Price_per_unit_USD'] = "NA"


            # PRIMARY IMAGE
            primary_image = response.xpath('//*[@id="ppd"]//span[@class="a-list-item"]/span/div/img/@src').get()
            supplementary_images_and_video = response.xpath('//*[@id="ppd"]//div[@id="altImages"]//img[contains(@src,".jpg")]/@src|//div[@id="main-video-container"]//video/@src').getall()
            if primary_image:
                match = re.findall(r'SX\d+_SY\d+', primary_image, flags=re.IGNORECASE)
                if match:
                    primary_image = primary_image.replace(match[0],'SX900_SY900')
                image_url = [primary_image.replace('300_SY300_QL70_ML2_', '679_').replace('SX300', 'SX').replace('SY300', 'SY').replace('SX679', 'SX').replace('SR38,50','SR900,900')]
                if supplementary_images_and_video:
                    supplementary_images = '|'.join(supplementary_images_and_video).replace('SX', '').replace('SY', '').replace('_AC_US40_', '').replace('SR38,50','SR900,900')
                    supplementary_images = supplementary_images.split('|')
                    item['Image_Urls'] = ' | '.join(image_url+supplementary_images)
                else:item['Image_Urls'] = ''.join(image_url)
            else:item['Image_Urls'] = "NA"

            if not item['ABV']:
                abv_test = response.xpath("//span[contains(text(),'Alcohol content')]/text()").get()
                if abv_test:
                    abv_reg = re.search(r'(\d+)%',abv_test,re.IGNORECASE)
                    if abv_reg:
                        item['ABV'] = abv_reg.group(1)


            if item['ABV'] == '100':item['ABV'] = ''
            for key in item.keys():
                if not item[f'{key}']:
                    item[f'{key}'] = 'NA'

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
            yield item
            # for key in item.keys():
            #     japanese_text = item[key]
            #     if isinstance(japanese_text, str):
            #         english_translation = translate_polish_to_english(japanese_text)
            #         item[key] = english_translation
            # print(item)
            # except Exception as e:
            #     self.cursor.execute(f"""UPDATE {db.db_links_table} SET STATUS='Error' WHERE product_id='{item['Product_id']}'""")
            #     self.con.commit()
            #     print(e)
            #     print('Error')

    def closed(self, reason):
        """This method is called when the spider finishes."""
        print("Spider closed, reason:", reason)
        file_generation()  # Call your file generation function here

if __name__ == '__main__':
    # execute("scrapy crawl data_extractor -a start=8911 -a end=19128".split())
    execute("scrapy crawl data_extractor -a start=1 -a end=1".split())