import os
from datetime import datetime

# DATABASE DETAILS
month_date = '16102024'

db_host = '172.27.132.55'
db_user = 'root'
db_password = 'actowiz'
db_name = 'metrobg'

db_links_table = 'product_link'
# db_data_table = 'all_city_4_data'
db_data_table = f'product_data_{month_date}'
PAGESAVE = f'D:/PAGESAVE/METRO_BG/{month_date}'
# PAGESAVE = f'C:/PAGESAVE/METRO_BG/{month_date}'

def pagesave(response,product_id):
    if not os.path.exists(PAGESAVE):
        os.makedirs(PAGESAVE)
    with open(f"{PAGESAVE}/{product_id}.html", 'w', encoding='utf-8') as f:
        f.write(response.text)