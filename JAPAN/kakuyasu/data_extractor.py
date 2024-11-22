import re
from parsel import Selector
from db_config import *
from EXPORT import file_generation

con1 = pymysql.connect(host=db_host, user=db_user, passwd=db_password)
cur1 = con1.cursor()

def product_details(product_link, id,type):
    product_id = product_link.split('/')[-2].strip()
    response_text = ''
    try:
        # with open(f'C:\\PAGESAVE\\KAKUYASU\\{month_date}\\{product_id}.html','r',encoding='utf-8') as f:
        with open(f"{PAGESAVE}/{product_id}.html",'r',encoding='utf-8') as f:
            response_text = f.read()
            res = Selector(text=response_text)
    except Exception as e:
        response_text = ''
        print(e)
        return None
    if "お探しの商品が見つかりません" not in response_text:
        try:
            item = {}
            item['type'] = type
            item['Platform_Name'] = 'kakuyasu'
            item['Platform_URL'] = product_link
            item['Product_ID'] = product_id
            # item['Category'] = driver.find_elements(By.XPATH, '//*[@id="topicPath"]//ul/li//font/font')[-2].text.title()
            item['Category'] = res.xpath('//div[@id="topicPathSP"]/div[@class="bread-hd"]/ul/li')[-2].xpath('a/text()').get()
            item['SKU_Name'] = res.xpath('(//h1[@id="commodityName"])[1]/text()').get()
            # item['SKU_Name'] = driver.find_elements(By.XPATH, '//*[@id="topicPath"]//ul/li//ancestor::li')[-1].text.strip()
            item['Country'] = 'Japan'
            item['Major_Region'] = 'Asia Pacific'
            item['Market_Clusters'] = 'APAC Develop'
            item['Price_In_Local_USD'] = 'NA'


            try:
                item['Pack_Size_Local'] = res.xpath('//table[@class="code-table02"]//tr//*[contains(text(),"容量")]/../../following-sibling::span/text()').get()
                if '*' in item['Pack_Size_Local']:
                    item['Pack_Size_Local'] = item['Pack_Size_Local'].split('*')[0].strip()
            except:
                try:
                    item['Pack_Size_Local'] = res.xpath('//table[@class="code-table02"]//tr//*[contains(text(),"容量")]/following-sibling::span/text()').get()
                except:
                    try:
                        item['Pack_Size_Local'] = res.xpath('//table[@class="code-table02"]//tr//*[contains(text(),"容量")]/../../following-sibling::span/text()').get()
                    except:
                        item['Pack_Size_Local'] = 'NA'

            if item['Pack_Size_Local'] and item['Pack_Size_Local'] != 'NA':
                pack_size_local_lower = item['Pack_Size_Local'].lower()
                unit_mapping = {'x': 1, 'cl': 10, 'ml': 1, 'litre': 1000, 'lt': 1000, 'l': 1000}

                for unit, multiplier in unit_mapping.items():
                    if unit in pack_size_local_lower:
                        if '.' in pack_size_local_lower:
                            value = float(re.findall('(\d+.\d+)', pack_size_local_lower)[0])
                        else:
                            value = float(re.findall('(\d+)', pack_size_local_lower)[0])

                        item['Pack_Size'] = str(int(value * multiplier))
                        break
                else:
                    item['Pack_Size'] = 'NA'
            else:
                # item['Pack_Size_Local'] = 'NA'
                item['Pack_Size'] = 'NA'

            try:
                before_price = res.xpath('(//span[@class="normal-price"]/span[@class="price" or @class="price-before"])[1]/text()').get()
            except:
                try:
                    before_price = res.xpath('(//span[@class="normal-price"]/span[@class="price" or @class="price-before"])[1]/text()').get()
                except:
                    before_price = ''

            if before_price:
                item['Price_In_Local'] = before_price.replace('yen', '').strip().replace('円','').replace(',', '')
                item['Price_In_Local_USD'] = convert_currency('YEN', float(item['Price_In_Local'].replace('円','').replace(',', '')))
                try:
                    item['Promo_Price_Local'] = res.xpath('(//span[@class="normal-price"]/span[@class="price-after"]/span)[1]/text()').get().replace('yen', '').replace('円','').replace(',', '').strip()
                    if '(' in item['Promo_Price_Local']:
                        item['Promo_Price_Local'] = item['Promo_Price_Local'].split('(')[0].replace('円','').replace(',', '').strip()
                    item['Promo_Price_USD'] = convert_currency('YEN', float(item['Promo_Price_Local'].replace('円','').replace(',', '')))
                except:
                    pass
            else:
                try:
                    item['Price_In_Local'] = res.xpath('//*[@id="recommend-checked1-1"]/following-sibling::p//span[@class="price"]/text()').get().replace('yen', '').replace('円','').replace(',', '').strip()
                    item['Price_In_Local_USD'] = convert_currency('YEN', float(item['Price_In_Local'].replace('円','').replace(',', '')))
                except:
                    item['Promo_Price_Local'] = 'NA'
                    item['Price_In_Local_USD'] = 'NA'

            try:
                item['Age_of_Whiskey'] = re.findall('(\d+) 年.*?', item['SKU_Name'].lower())[0].strip()
            except:
                try:
                    item['Age_of_Whiskey'] = re.findall('(\d+)年.*?', item['SKU_Name'].lower())[0].strip()
                except:
                    try:
                        item['Age_of_Whiskey'] = re.findall('(\d+?)よー?', item['SKU_Name'].lower())[0].strip()
                    except:
                        try:
                            item['Age_of_Whiskey'] = re.findall('(\d+?) よー?', item['SKU_Name'].lower())[0].strip()
                        except:
                            item['Age_of_Whiskey'] = 'NA'

            try:
                item['Country_of_Origin'] = res.xpath('//*[@class="code-table02"]//*[contains(text(),"産地")]/following-sibling::span/text()').get()
            except:
                try:
                    item['Country_of_Origin'] = res.xpath('//*[@class="code-table02"]//*[contains(text(),"産地")]/../../following-sibling::span/text()').get().strip().replace(
                        ':', '').strip()
                except:
                    item['Country_of_Origin'] = 'NA'

            item['Pack_Type'] = 'Bottle'

            try:
                image_list = []
                images = res.xpath('//*[@class="swiper-slide swiper-slide-thumb-active"]/img/@src').getall()
                if images:
                    for img in images:
                        # image_list.append(img.get_attribute('src'))
                        image_list.append('https://www.kakuyasu.co.jp'+img)
                    item['Image_Urls'] = ' | '.join(image_list)
                else:
                    images1 = res.xpath('//*[@class="image-block-main"]/@src').get()
                    item['Image_Urls'] = 'https://www.kakuyasu.co.jp'+images1
            except:
                item['Image_Urls'] = 'NA'

            try:
                item['ABV'] = res.xpath('//*[contains(@class, "code-table02")]//tr//*[contains(text(),"度数 ") or contains(text(),"度数")]/following-sibling::span/text()').get().replace(
                    '度', '').strip()
            except:
                item['ABV'] = 'NA'

            try:
                item['scrape_date'] =datetime.now().strftime("%d_%m_%Y")
            except:
                item['scrape_date'] = datetime.now().strftime("%d_%m_%Y")
            item['Standard_Currency'] = 'USD'
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

            insert_into_table(item=item, table_name=db_data_table)
            update_table(table_name=db_links_table, set_values='Status="Done"', condition=f'id={id}')
        except Exception as e:
            print(e,item['Platform_URL'])
            # driver.close()
    else:
        update_table(table_name=db_links_table, set_values='Status="Not Found"', condition=f'id={id}')

def main():
    con = connection()
    cur = con.cursor()
    query = f"select * from {db_links_table} where Status='SAVE'"
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    con.close()
    print(f'len of links......{len(data)}')
    for d in data:
        id = d[0]
        product_link = d[1]
        type = d[-1]
        product_details(product_link=product_link, id=id,type=type)

    try:
        file_generation()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    create_table()
    main()
