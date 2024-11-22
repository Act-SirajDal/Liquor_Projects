import datetime
import os.path
import numpy as np
import pandas as pd
import yamaya.db_config as db
import pymysql

conn = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
df = pd.read_sql(f"select * from {db.db_data_table}", conn)
path = f"D:\\BACKUP\\HINAL\\DATA\\YAMAYA\\{db.month_date}\\"
if not os.path.exists(path):
    os.makedirs(path)
with pd.ExcelWriter(path + f'YAMAYA_{db.month_date}.xlsx', engine='xlsxwriter') as writer:
    try:
        print(len(df))
        del df['Id']


        df.insert(0, 'Id', range(1, len(df) + 1))
        df.replace(np.NaN, 'NA', inplace=True)
        df.replace('', 'NA', inplace=True)
        df.to_excel(writer, sheet_name='data', index=False,header=True)
        print("FILE GENRATED SUCCESSFULLY.....")
    except Exception as e:
        print(e)
        print("ERROR GENRATED")