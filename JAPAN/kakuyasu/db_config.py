import os
from datetime import datetime

import pymysql
from pymysql import IntegrityError

# DATABASE DETAILS
month_date = '17102024'

db_host = '172.27.132.55'
# db_host = 'localhost'
db_user = 'root'
db_password = 'actowiz'
db_name = 'kakuyasu'
db_links_table = 'spirit_link'
db_data_table = f'spirit_data_{month_date}'
db_manufacturers_table = 'manufactures'
db_master_table = 'master_sheet'
db_cluster_table = 'clusters'
# PAGESAVE = f'C:/PAGESAVE/KAKUYASU/{month_date}'
PAGESAVE = f'D:/PAGESAVE/KAKUYASU/{month_date}'
con = pymysql.connect(host=db_host, user=db_user, passwd=db_password,database=db_name)
cur = con.cursor()

def convert_currency(currency_type, value):
    exchange_rates = {
        'GBP': 1.2000,
        'EUR': 1.0500,
        'CAN': 0.7874,
        'RMB/CNY': 0.1466,
        'AUD': 0.7246,
        'EUR to RMB/CNY': 7.1610,
        'YEN': 0.0068
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
        f.write(response)

def create_database():
    con = pymysql.connect(host=db_host, user=db_user, passwd=db_password, database=db_name)
    cur = con.cursor()
    """Function to create the database if it doesn't exist."""
    cur.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
    cur.close()
    con.close()


def connection():
    """
    Function to establish a connection to the database.
    """
    con = pymysql.connect(host=db_host, user=db_user, passwd=db_password, database=db_name)
    cur = con.cursor()
    return con


def create_table():
    """
    Function to create a table in the database if it doesn't exist.
    """
    con = pymysql.connect(host=db_host, user=db_user, passwd=db_password, database=db_name)
    cur = con.cursor()
    try:
        # Add your table query here
        links_table_query = f"""CREATE TABLE IF NOT EXISTS {db_links_table} (
                                     Id INT NOT NULL AUTO_INCREMENT,
                                     Product_URL VARCHAR(255) UNIQUE DEFAULT NULL ,
                                     product_id VARCHAR(255),
                                     Status VARCHAR(10) DEFAULT 'Pending',
                                     PRIMARY KEY (Id)
                                   ) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;"""
        cur.execute(links_table_query)
    except Exception as e:
        print(e)
        pass

    try:
        # Add your table query here
        data_table_query = f"""CREATE TABLE IF NOT EXISTS `{db_data_table}` (`Id` INT NOT NULL AUTO_INCREMENT,
                                                                                       `Platform_Name` varchar(250) DEFAULT 'NA',
                                                                                       `Platform_URL` varchar(250) DEFAULT 'NA',
                                                                                       `Product_id` varchar(20) DEFAULT 'NA',
                                                                                       `Category` varchar(250) DEFAULT 'NA',
                                                                                       `Sub_Category` varchar(250) DEFAULT 'NA',
                                                                                       `Sector` varchar(250) DEFAULT 'NA',
                                                                                       `Sub_Sector` varchar(250) DEFAULT 'NA',
                                                                                       `SKU_Name` varchar(250) DEFAULT 'NA',
                                                                                       `Manufacturer` varchar(250) DEFAULT 'NA',
                                                                                       `Brand` varchar(250) DEFAULT 'NA',
                                                                                       `Sub_Brand` varchar(250) DEFAULT 'NA',
                                                                                       `Origin` varchar(250) DEFAULT 'NA',
                                                                                       `Major_Region` varchar(250) DEFAULT 'NA',
                                                                                       `Country` varchar(250) DEFAULT 'NA',
                                                                                       `Market_Clusters` varchar(250) DEFAULT 'NA',
                                                                                       `Pack_Size_Local` varchar(250) DEFAULT 'NA',
                                                                                       `Pack_Size` varchar(250) DEFAULT 'NA',
                                                                                       `Pack_Size_New` varchar(250) DEFAULT 'NA',
                                                                                       `Price_In_Local` varchar(250) DEFAULT 'NA',
                                                                                       `Price_In_Local_USD` varchar(250) DEFAULT 'NA',
                                                                                       `Price_Range` varchar(250) DEFAULT 'NA',
                                                                                       `Type_of_Promo` varchar(250) DEFAULT 'NA',
                                                                                       `Promo_Price_Local` varchar(250) DEFAULT 'NA',
                                                                                       `Promo_Price_USD` varchar(250) DEFAULT 'NA',
                                                                                       `Price_per_unit_Local` varchar(250) DEFAULT 'NA',
                                                                                       `Price_per_unit_USD` varchar(250) DEFAULT 'NA',
                                                                                       `Price_per_unit_USD_New` varchar(250) DEFAULT 'NA',
                                                                                       `Age_of_Whiskey` varchar(250) DEFAULT 'NA',
                                                                                       `Country_of_Origin` varchar(250) DEFAULT 'NA',
                                                                                      `Distillery` varchar(250) DEFAULT 'NA',  
                                                                                       `Pack_type` varchar(250) DEFAULT 'NA',
                                                                                       `Tasting_Notes` text,
                                                                                       `Image_Urls` text,
                                                                                       `ABV` varchar(50) DEFAULT 'NA',
                                                                                       `scrape_date` varchar(50),   
                                                                                       `Standard_Currency` varchar(255) DEFAULT 'USD', 
                                                                                       `type` varchar(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,  
                                                                                       UNIQUE KEY `pid` (`Product_id`),
                                                                                       PRIMARY KEY (`Id`)
                                                                                     ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4"""
        cur.execute(data_table_query)
    except Exception as e:
        print(e)
        pass

    con.commit()
    con.close()

def insert_into_table(item, table_name):
    """
    Function to insert data into the table.
    Arguments:
        - item: Dictionary containing the data to be inserted.
        - table_name: Name of the table to insert data into.
    """
    con = pymysql.connect(host=db_host, user=db_user, passwd=db_password, database=db_name)
    cur = con.cursor()
    field_list = []
    value_list = []
    for field in item:
        field_list.append(str(field).strip())
        value_list.append(str(item[field]).replace("'", "’").strip())

    fields = ','.join(field_list)
    values = "','".join(value_list)
    insert_db = f"INSERT IGNORE INTO {table_name} " + "(" + fields + ") VALUES ('" + values + "')"

    try:
        cur.execute(insert_db)
        con.commit()
        print('Data Inserted')
    except Exception as e:
        print(e)
        pass

    cur.close()
    con.close()



def update_table(table_name, set_values, condition=''):
    """
    Function to update data in the table.
    Arguments:
        - table_name: Name of the table to update.
        - set_values: Values to set in the update statement.
        - condition: Condition to apply while updating data, defaults to empty string.
    """
    con = pymysql.connect(host=db_host, user=db_user, passwd=db_password, database=db_name)
    cur = con.cursor()
    try:
        update_query = f"UPDATE {table_name} SET {set_values}"
        if condition:
            update_query += f" WHERE {condition}"
        cur.execute(update_query)
        con.commit()
        print('Table updated')
    except Exception as e:
        print(e)
        pass

    cur.close()
    con.close()

