import datetime
import os.path
import pandas as pd
import kaufland.db_config as db
import pymysql
from googletrans import Translator
from concurrent.futures import ThreadPoolExecutor
import numpy as np

month_date = db.month_date


def translate_chunk(chunk, columns):
    """Translate a chunk of the dataframe with progress tracking and error handling."""
    translator = Translator()

    for col in columns:
        if col in chunk.columns:
            print(f"Translating chunk for column: {col}...")
            unique_values = chunk[col].dropna().unique()
            translation_dict = {}
            for value in unique_values:
                try:
                    if isinstance(value, str) and value.strip().lower() == 'na':
                        translation_dict[value] = 'Na'
                    elif isinstance(value, str) and value.strip() != '':
                        translation_dict[value] = translator.translate(value).text
                        print(f"Translated: '{value}' -> '{translation_dict[value]}'")
                except Exception as e:
                    print(f"Error translating '{value}' in chunk: {e}")
                    translation_dict[value] = value  # Keep original value if error occurs

            chunk[col] = chunk[col].map(translation_dict).fillna(chunk[col])
    return chunk


def translate_column_parallel(df, columns, n_workers=4):
    """Helper function to translate specified columns in the dataframe using parallel processing."""
    chunks = np.array_split(df, n_workers)
    print(f"Dataframe split into {n_workers} chunks for parallel processing.")

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        results = list(executor.map(lambda chunk: translate_chunk(chunk, columns), chunks))

    print("All chunks processed.")
    return pd.concat(results)


def file_generation():
    print("File Geneation process start....")
    today = datetime.datetime.strftime(datetime.date.today(), '%d%m%y')

    # Connect to the database
    conn = pymysql.connect(host=db.db_host,user=db.db_user,password=db.db_password,database=db.db_name)

    # Read data from the database table
    # df = pd.read_sql(f"select * from {db.db_data_table} WHERE Category_2='Spirits'", conn)
    df = pd.read_sql(f"select * from {db.db_data_table}", conn)

    # Define the output path
    path = f"D:\\BACKUP\\HINAL\\DATA\\KAUFLAND_BG\\{month_date}\\"
    # path = f"C:\\BACKUP\\HINAL\\DATA\\KAUFLAND\\BG\\{month_date}\\"
    if not os.path.exists(path):
        os.makedirs(path)

    # Specify columns to translate
    columns_to_translate = ["SKU_Name", "Pack_Size_Local"]
    # columns_to_translate = ["SKU_Name"]

    # Translate the specific columns from Bulgarian ('bg') to English ('en')
    df = translate_column_parallel(df, columns_to_translate,n_workers=4)

    # Create an Excel writer and save the dataframe to an Excel file
    pd.options.display.float_format = '{:.0f}'.format
    # with pd.ExcelWriter(path + f'{db.db_data_table}.xlsx', engine='xlsxwriter',options={'strings_to_urls': False}) as writer:
    with pd.ExcelWriter(path + f'KAUFLAND_BG_{month_date}.xlsx', engine='xlsxwriter') as writer:
        try:
            print(len(df))
            # df.insert(0, 'Id', range(1, len(df) + 1))
            df = df.drop('Id', axis=1, errors='ignore')
            df['Id'] = df.reset_index().index + 1
            df.insert(0, 'Id', df.pop('Id'))

            df.fillna('NA', inplace=True)
            df.replace('', 'NA', inplace=True)
            # del df['status']
            # del df['Sheet_Brand']
            # df.replace(r'\s+', ' ', regex=True, inplace=True)
            df.to_excel(writer, sheet_name='data', index=False,header=True)
            print("FILE PATH : ", path)
            print("FILE GENRATED SUCCESSFULLY.....")
        except Exception as e:
            print(e)
