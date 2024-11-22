import datetime
import os.path
import numpy as np
import pandas as pd
import likaman.db_config as db
import pymysql

conn = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
df = pd.read_sql(f"select * from {db.db_data_table}", conn)
path = f'D:\\BACKUP\\HINAL\\DATA\\LIKAMAN\\{db.month_date}\\'

if not os.path.exists(path):
    os.makedirs(path)
with pd.ExcelWriter(path + f'LIKAMAN_{db.month_date}.xlsx', engine='xlsxwriter', engine_kwargs={"options":{'strings_to_urls': False}}) as writer:
    try:
        print(len(df))

        df = df.drop('Id', axis=1, errors='ignore')
        df['Id'] = df.reset_index().index + 1
        df.insert(0, 'Id', df.pop('Id'))

        df.replace(np.NaN, 'NA', inplace=True)
        df.replace('', 'NA', inplace=True)
        df.to_excel(writer, sheet_name='data', index=False, header=True)
        print("FILE GENRATED SUCCESSFULLY.....")
    except:
        print("ERROR GENRATED")
