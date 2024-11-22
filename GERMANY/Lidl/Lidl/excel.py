import datetime
import os.path
import pandas as pd
import Lidl.db_config as db
import pymysql
today = datetime.datetime.strftime(datetime.date.today(), '%d%m%y')
conn = pymysql.connect(host=db.db_host,user=db.db_user,password=db.db_password,database=db.db_name)
df = pd.read_sql(f"select * from {db.db_data_table}", conn)
path = f"D:\\BACKUP\\HINAL\\DATA\\LIDL\\{db.month_date}\\"

if not os.path.exists(path):
    os.makedirs(path)
with pd.ExcelWriter(path + f'Lidl_{db.month_date}.xlsx', engine='xlsxwriter',) as writer:
    try:
        # del df['Category_1']
        # del df['Category_2']
        print(len(df))

        df = df.drop('id', axis=1, errors='ignore')
        df['id'] = df.reset_index().index + 1
        df.insert(0, 'id', df.pop('id'))

        df.fillna('NA', inplace=True)
        df.replace('', 'NA', inplace=True)
        df.to_excel(writer, sheet_name='data', index=False,header=True)
        print("FILE GENRATED SUCCESSFULLY.....")
    except:
        print("ERROR GENRATED")
