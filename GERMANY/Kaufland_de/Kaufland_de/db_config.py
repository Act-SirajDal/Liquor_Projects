import os
import re


month_date = '09122024'
db_host = '172.27.132.55'
# db_host = 'localhost'
db_user = 'root'
db_password = 'actowiz'
db_name = 'kaufland_de'
db_links_table = 'spirit_links'
# db_data_table = f'SPIRIT_DATA_{month_date}'
db_data_table = f'spirit_data_{month_date}'
db_master_table = 'spirits_mapping_details'
# PAGESAVE = f'D:/PAGESAVE/KAUFLAND_DE/{month_date}'
PAGESAVE = f'C:/PAGESAVE/KAUFLAND_DE/{month_date}'


def pack_size(self, value):
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
    if Pack_Size:
        return Pack_Size, Web_Pack_Size
    else:
        return None

def pagesave(response,product_id):
    if not os.path.exists(PAGESAVE):
        os.makedirs(PAGESAVE)
    with open(f"{PAGESAVE}/{product_id}.html", 'w', encoding='utf-8') as f:
        f.write(response.text)