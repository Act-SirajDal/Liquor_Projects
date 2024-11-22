import os
import re

month_date = '16102024'

# db_host = 'localhost'
db_host = '172.27.132.55'
db_user = 'root'
db_password = 'actowiz'
db_name = 'Glovoapp_test'
db_links_table = 'SPIRIT_LINKS'
db_data_table = f'SPIRIT_DATA_Glovoap_Billa_{month_date}'
db_master_table = 'spirits_mapping_details'

# PAGESAVE = fr'D:/PAGESAVE/Glovoap-Billa/{month_date}'
PAGESAVE = fr'C:/PAGESAVE/Glovoap-Billa/{month_date}'

def pack_size(value):
    Pack_Size = None
    Web_Pack_Size = None
    if re.findall('([-+]?(?:\d*\.*\d+))\s*cl', value, flags=re.IGNORECASE):
        pack = re.findall('([-+]?(?:\d*\.*\d+))(\s*cl)', value, flags=re.IGNORECASE)
        if pack:
            ps = pack[0][0]
            unit = pack[0][1]
            pack = float(ps)
            Pack_Size = round(pack * 10, 2)
            Web_Pack_Size = f'{ps}{unit}'
    elif re.findall('([-+]?(?:\d*\.*\d+))\s*ml', value, flags=re.IGNORECASE):
        pack = re.findall('([-+]?(?:\d*\.*\d+))(\s*ml)', value, flags=re.IGNORECASE)
        if pack:
            ps = pack[0][0]
            unit = pack[0][1]
            pack = float(ps)
            Pack_Size = round(pack, 2)
            # Web_Pack_Size = f'{ps} ml'
            Web_Pack_Size = f'{ps}{unit}'
    elif re.findall('([-+]?(?:\d*\.*\d+))\s*l', value, flags=re.IGNORECASE):
        pack = re.findall('([-+]?(?:\d*\.*\d+))(\s*l)', value, flags=re.IGNORECASE)
        if pack:
            ps = pack[0][0]
            unit = pack[0][1]
            pack = float(ps)
            Pack_Size = round(pack * 1000, 2)
            Web_Pack_Size = f'{ps}{unit}'
    if Pack_Size:return Pack_Size, Web_Pack_Size
    else:return None

def pagesave(response,product_id):
    if not os.path.exists(PAGESAVE):
        os.makedirs(PAGESAVE)
    with open(f"{PAGESAVE}/{product_id}.html", 'w', encoding='utf-8') as f:
        f.write(response.text)