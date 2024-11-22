# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymysql
from Lidl.items import *
import Lidl.db_config as db


class LidlPipeline:
    conn = pymysql.connect(
        host=db.db_host,
        user=db.db_user,
        password=db.db_password,
        db=db.db_name)
    mycursor = conn.cursor()

    # Create table if it does not exist
    create_table_query = f'''CREATE TABLE IF NOT EXISTS {db.db_links_table}
                         (id INT AUTO_INCREMENT PRIMARY KEY,
                         url varchar(555),
                         Product_id varchar(555) unique
                         )'''

    mycursor.execute(create_table_query)

    # Create table if it does not exist
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
                         `Pack_Size_New` VARCHAR(55),
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
                         `Tasting_Notes` LONGTEXT ,
                         `Image_Urls` LONGTEXT,
                         `ABV` VARCHAR(55) DEFAULT NULL,
                         `scrape_date` VARCHAR(55),
                         `Standard_Currency` VARCHAR(55),
                          `type` VARCHAR(55)

                         )'''

    mycursor.execute(create_table_query)

    def process_item(self, item, spider):

        if isinstance(item, LidlItem):

            field_list = []
            value_list = []
            for field in item:
                field_list.append(str(field))
                value_list.append('%s')
            fields = ','.join(field_list)
            values = ", ".join(value_list)
            insert_db = f"insert into {db.db_links_table}( " + fields + " ) values ( " + values + " )"
            try:
                self.mycursor.execute(insert_db, tuple(item.values()))
                self.conn.commit()
                print("Data insert")

            except Exception as e:
                print(e)
            return item

        if isinstance(item, Lidl_dataItem):

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

                ins2 = f"""UPDATE {db.db_links_table} SET `status` = 'Done' WHERE `url` ='{item["Platform_URL"]}'"""
                self.mycursor.execute(ins2)
                self.conn.commit()
                print("Done.......")
            except Exception as e:
                print(e)
        return item