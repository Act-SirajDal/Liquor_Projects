import os
from datetime import datetime

month_date = '16102024'

# DATABASE DETAILS
db_host = '172.27.132.55'
# db_host = 'localhost'
db_user = 'root'
db_password = 'actowiz'
db_name = 'edeka24'
db_links_table = 'SPIRIT_LINKS'
db_data_table = f'SPIRIT_DATA_{month_date}'
db_manufacturers_table = 'manufactures'
db_master_table = 'master_sheet'
db_cluster_table = 'clusters'
PAGESAVE = fr"D:/PAGESAVE/EDEKA/{month_date}"
# PAGESAVE = fr"C:/PAGESAVE/EDEKA/{month_date}"


def convert_currency(currency_type, value):
    exchange_rates = {
        'GBP': 1.2000,
        'EUR': 1.1500,
        'CAN': 0.7874,
        'RMB/CNY': 0.1466,
        'AUD': 0.7246,
        'EUR to RMB/CNY': 7.1610,
        'PLN': 0.24
    }

    conversion_rate = exchange_rates.get(currency_type)

    if conversion_rate is None:
        return 'Invalid currency type'

    converted_value = '{:.2f}'.format(value * conversion_rate)
    return converted_value

def pagesave(response,product_id):
    if not os.path.exists(PAGESAVE):
        os.makedirs(PAGESAVE)
    with open(f"{PAGESAVE}/{product_id}.html", 'w', encoding='utf-8') as f:
        f.write(response.text)