# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymysql
from glovoapp.items import *
import glovoapp.db_config as db


class GlovoappPipeline:
    conn = pymysql.connect(
        host=db.db_host,
        user=db.db_user,
        password=db.db_password)
    mycursor = conn.cursor()

    # SQL query to create a database if it doesn't exist
    create_db_query = f"CREATE DATABASE IF NOT EXISTS {db.db_name}"

    # Execute the query
    mycursor.execute(create_db_query)

    # Close the cursor and connection
    mycursor.close()
    conn.close()


    conn = pymysql.connect(
        host=db.db_host,
        user=db.db_user,
        password=db.db_password,
        db=db.db_name)
    mycursor = conn.cursor()

    create_table_query = f'''CREATE TABLE IF NOT EXISTS {db.db_data_table}
                           (id INT AUTO_INCREMENT PRIMARY KEY,
                          `Platform_Name` VARCHAR(555),
                         `Platform_URL` VARCHAR(255),
                         `Product_id` VARCHAR(55) UNIQUE,
                         `Category` VARCHAR(55),
                         `Sub_Category` VARCHAR(255),
                         `Sector` VARCHAR(55),
                         `Sub_Sector` VARCHAR(55),
                         `SKU_Name` VARCHAR(255),
                         `Manufacturer` VARCHAR(255),
                         `Brand` VARCHAR(255),
                         `Sub_Brand` VARCHAR(255),
                         `Origin` VARCHAR(55),
                         `Major_Region` VARCHAR(55),
                         `Country` VARCHAR(55),
                         `Market_Clusters` VARCHAR(55),
                         `Pack_Size_Local` VARCHAR(55),
                         `Pack_Size` VARCHAR(55),
                         `Pack_Size_New` varchar(250),
                         `Price_In_Local` VARCHAR(55),
                         `Price_In_Local_USD` VARCHAR(55),
                         `Price_Range` VARCHAR(55),
                         `Type_of_Promo` VARCHAR(55),
                         `Promo_Price_Local` VARCHAR(55),
                         `Promo_Price_USD` VARCHAR(55),
                         `Price_per_unit_Local` VARCHAR(55),
                         `Price_per_unit_USD` VARCHAR(55),
                         `Price_per_unit_USD_New` VARCHAR(55),
                         `Age_of_Whiskey` VARCHAR(55),
                         `Country_of_Origin` VARCHAR(55),
                         `Distillery` VARCHAR(255),
                         `Pack_type` VARCHAR(55),
                         `Tasting_Notes` LONGTEXT,
                         `Image_Urls` LONGTEXT,
                         `ABV` VARCHAR(55) DEFAULT NULL,
                         `scrape_date` VARCHAR(55),
                         `Standard_Currency` VARCHAR(55)
                         )'''

    mycursor.execute(create_table_query)

    def process_item(self, item, spider):
        if isinstance(item, Glovoapp_dataItem):

            field_list = []
            value_list = []
            for field in item:
                field_list.append(str(field))
                value_list.append('%s')
            fields = ','.join(field_list)
            values = ", ".join(value_list)
            insert_db = f"insert into {db.db_data_table}( " + fields + " ) values ( " + values + " )"
            try:
                self.mycursor.execute(insert_db, tuple(item.values()))
                self.conn.commit()
                print("Data insert")
            except Exception as e:
                print(e)
        return item