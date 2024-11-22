import os
import re

month_date = '08102024'

db_host = '172.27.132.55'
db_user = 'root'
db_password = 'actowiz'
db_name = f'kaufland_bg_1110'
db_links_table = f'SPIRIT_LINKS_{month_date}'
db_data_table = f'SPIRIT_DATA_{month_date}'
# db_data_table = 'spirit_data_05032024'

PAGESAVE = f'D:/PAGESAVE/KAUFLAND_BG/{month_date}/'
# PAGESAVE = f'C:/PAGESAVE/KAUFLAND/BG/{month_date}'

def pagesave(response,product_id):
    if not os.path.exists(PAGESAVE):
        os.makedirs(PAGESAVE)
    with open(f"{PAGESAVE}/{product_id}.html", 'w', encoding='utf-8') as f:
        f.write(response.text)