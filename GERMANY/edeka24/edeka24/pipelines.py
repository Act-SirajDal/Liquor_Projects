# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymysql import IntegrityError

import edeka24.db_config as db
from edeka24.items import Edeka24Links, Edeka24Item


class Edeka24Pipeline:
    data_insert = 0
    variation_insert = 0

    def open_spider(self, spider):

        try:
            create_database = f"CREATE DATABASE IF NOT EXISTS {db.db_name};"
            spider.cursor.execute(create_database)
            spider.cursor.execute(f"USE {db.db_name};")
        except Exception as e:
            spider.logger.info(e)

        try:
            create_table = f"""CREATE TABLE IF NOT EXISTS `{db.db_data_table}` (`Id` INT NOT NULL AUTO_INCREMENT,
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
                                                                                    `Pack_Size_New` VARCHAR(55),
                                                                                    `Price_In_Local` varchar(250) DEFAULT 'NA',
                                                                                    `Price_In_Local_USD` varchar(250) DEFAULT 'NA',
                                                                                    `Price_Range` VARCHAR(55),
                                                                                    `Type_of_Promo` varchar(250) DEFAULT 'NA',
                                                                                    `Promo_Price_Local` varchar(250) DEFAULT 'NA',
                                                                                    `Promo_Price_USD` varchar(250) DEFAULT 'NA',
                                                                                    `Price_per_unit_Local` varchar(250) DEFAULT 'NA',
                                                                                    `Price_per_unit_USD` varchar(250) DEFAULT 'NA',
                                                                                    `Price_per_unit_USD_New` varchar(250) DEFAULT 'NA',
                                                                                    `Age_of_Whisky` varchar(250) DEFAULT 'NA',
                                                                                    `Country_of_Origin` varchar(250) DEFAULT 'NA',
                                                                                     `Distillery` varchar(250) DEFAULT 'NA',

                                                                                    `Pack_type` varchar(250) DEFAULT 'NA',
                                                                                    `Tasting_Notes` text,
                                                                                    `Image_Urls` text,
                                                                                    `ABV` varchar(50) DEFAULT 'NA',
                                                                                    `scrape_date` varchar(50),   
                                                                                    `Standard_Currency` varchar(255) DEFAULT 'USD', 
                                                                                     `type` varchar(50),   
                                                                                    UNIQUE KEY `pid` (`Product_id`),
                                                                                    PRIMARY KEY (`Id`)
                                                                                  ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4"""
            spider.cursor.execute(create_table)
        except Exception as e:
            spider.logger.info(e)

        try:
            create_table = f"""CREATE TABLE IF NOT EXISTS {db.db_links_table} (
                                 Id INT NOT NULL AUTO_INCREMENT,
                                 category VARCHAR(255) DEFAULT NULL,
                                 Product_id VARCHAR(255) DEFAULT NULL,
                                 Product_URL VARCHAR(255) UNIQUE DEFAULT NULL ,
                                 Status VARCHAR(10) DEFAULT 'Pending',
                                 PRIMARY KEY (Id)
                               ) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;"""
            spider.cursor.execute(create_table)
        except Exception as e:
            spider.logger.info(e)

    def process_item(self, item, spider):
        if isinstance(item, Edeka24Item):
            try:
                Id = item.pop('Id')
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
                    spider.cursor.execute(insert_db, tuple(item.values()))
                    spider.con.commit()
                    self.data_insert += 1
                    spider.logger.info(f'Data Inserted...{self.data_insert}')
                except IntegrityError as e:
                    status = "Duplicate"

                update = f'update {db.db_links_table} set status="{status}" where Id=%s'
                # print(update)
                spider.cursor.execute(update, (Id))
                spider.con.commit()
                spider.logger.info('Done...')
            except Exception as e:
                spider.logger.error(e)

        if isinstance(item, Edeka24Links):
            try:
                # Id = item.pop('Id')
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_db = f"insert into {db.db_links_table}( " + fields + " ) values ( " + values + " )"

                try:
                    # status = "Done"
                    spider.cursor.execute(insert_db, tuple(item.values()))
                    spider.con.commit()
                    self.data_insert += 1
                    spider.logger.info(f'Data Inserted...{self.data_insert}')
                except IntegrityError as e:
                    status = "Duplicate"
                    print(e)

                # update = f'update {db.db_links_table} set status="{status}" where Id=%s'
                # print(update)
                # spider.cursor.execute(update, (Id))
                # spider.con.commit()
                # spider.logger.info('Done...')
            except Exception as e:
                spider.logger.error(e)

        return item
