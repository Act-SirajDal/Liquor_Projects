from datetime import datetime,timedelta
import os.path
import pandas as pd
import db_config as db
import pymysql

month_date = db.month_date
today = datetime.now().strftime('%Y%m%d')
conn = pymysql.connect(host=db.db_host,
                               user=db.db_user,
                               password=db.db_password,
                               database=db.db_name)
# path = f'C:\\Liquor\\DATA\\AMAZON_JP\\{month_date}\\'
path = f'D:\\HINAL\\DATA\\AMAZON_JP\\{month_date}\\'
cur = conn.cursor()
df = pd.read_sql(f"SELECT * from {db.db_data_table}", conn)
# del df['Category_1']

if not os.path.exists(path):
    os.makedirs(path)
with pd.ExcelWriter(path+ f'AMAZON_JP_new_{month_date}.xlsx', engine='xlsxwriter') as writer:
    try:
        print(len(df))
        df.fillna('NA', inplace=True)
        df.replace('', 'NA', inplace=True)
        df.to_excel(writer, sheet_name='data', index=False,header=True)
        print(f'AMAZON_{month_date}',"   File Generated Successfully")
    except:
        print("error generate")
