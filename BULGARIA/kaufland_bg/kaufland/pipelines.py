# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymysql import IntegrityError

import kaufland.db_config as db
from kaufland.items import *

class KauflandPipeline:
    data_insert = 0
    con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password)
    cursor = con.cursor()
    create_database = f"CREATE DATABASE IF NOT EXISTS {db.db_name} /*!40100 DEFAULT CHARACTER SET utf8mb4 */;"

    cursor.execute(create_database)
    cursor.execute(f"USE {db.db_name};")

    def process_item(self, item, spider):
        if isinstance(item, KauflandLink):
            self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS`{db.db_links_table}` (
                                                                                `Id` int(11) NOT NULL AUTO_INCREMENT,
                                                                                `url` varchar(555),
                                                                                `product_id` varchar(55),
                                                                                `status` varchar(255) DEFAULT 'pending',
                                                                                UNIQUE KEY `url` (`url`),
                                                                                PRIMARY KEY (`Id`)
                                                                              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            try:
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_db = f"insert into {db.db_links_table}( " + fields + " ) values ( " + values + " )"
                try:
                    self.cursor.execute(insert_db, tuple(item.values()))
                    self.con.commit()
                    self.data_insert += 1
                    spider.logger.info(f'Data Inserted...{self.data_insert}')
                except IntegrityError as e:
                    print(e)

            except Exception as e:
                spider.logger.error(e)

        if isinstance(item, KauflandData):
            self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS `{db.db_data_table}` (
                                                                                     `Id` INT(11) NOT NULL AUTO_INCREMENT,
                                                                                     `Platform_Name` VARCHAR(555),
                                                                                     `Platform_URL` VARCHAR(255),
                                                                                     `Product_id` VARCHAR(55),
                                                                                     `Category` VARCHAR(55),
                                                                                     `Category_1` VARCHAR(55),
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
                                                                                     `Standard_Currency` VARCHAR(55),
                                                                                     PRIMARY KEY (`Id`),
                                                                                     UNIQUE KEY `Product_id` (`Product_id`)
                                                                                   ) ENGINE=INNODB DEFAULT CHARSET=utf8mb4""")
            try:
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_db = f"insert into {db.db_data_table}( " + fields + " ) values ( " + values + " )"
                try:
                    status = "Done"
                    self.cursor.execute(insert_db, tuple(item.values()))
                    self.con.commit()
                    self.data_insert += 1
                    # spider.logger.info(f'Data Inserted...{self.data_insert}')
                    print(f'Data Inserted...{self.data_insert}')
                except IntegrityError as e:
                    status = "Duplicate"

                update = f'update {db.db_links_table} set status="{status}" where Id=%s'
                # print(update)
                self.cursor.execute(update, (item['Id']))
                self.con.commit()
                spider.logger.info('Done...')
            except Exception as e:
                spider.logger.error(e)

        return item
