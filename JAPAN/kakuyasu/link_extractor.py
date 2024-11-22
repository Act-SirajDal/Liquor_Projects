import requests
from lxml import html
from db_config import *


def parse_category(category_id):
    page = 1
    while True:
        params = {'pageSize': '12', 'currentPage': f'{page}', }
        pl_file_path = fr"{PAGESAVE}/Kakuyasu_PL_{page}_{category_id}.html"
        if os.path.exists(pl_file_path):
            response = open(pl_file_path,'r',encoding='utf-8').read()
        else:
            response = requests.get(f'https://www.kakuyasu.co.jp/store/category/{category_id}/', params=params)
            response = response.text
        doc = html.fromstring(response)
        all_products = doc.xpath('//*[@class="product-container"]//h3/a/@href')
        if all_products:
            if not os.path.exists(pl_file_path):
                product_id_name = f"Kakuyasu_PL_{page}_{category_id}"
                pagesave(response, product_id_name)
            else:
                print("File Already Available...")

            for url in all_products:
                product_url = base_url + url
                # product_id =product_url.split('/')[-2].strip('0')
                product_id =product_url.split('/')[-2]
                # item['product_id'] = product_id
                item = {'Product_URL': product_url,'product_id':product_id}
                insert_into_table(item=item, table_name=db_links_table)
            page += 1
        else:
            break


if __name__ == '__main__':
    create_database()
    create_table()
    base_url = 'https://www.kakuyasu.co.jp'
    category_id_list = ['001008', '001007', '001006']
    for cat_id in category_id_list:
        parse_category(cat_id)
