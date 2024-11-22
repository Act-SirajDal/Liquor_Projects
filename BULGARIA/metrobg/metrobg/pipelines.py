# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from pymysql import IntegrityError
import metrobg.db_config as db
from metrobg.items import MetrobgdataItem,MetrobglinkItem

class MetrobgPipeline:
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
            create_table = f"""CREATE TABLE IF NOT EXISTS `{db.db_links_table}` (`Id` INT NOT NULL AUTO_INCREMENT,
                                                                        url  VARCHAR(255),
                                                                        `product_id` varchar(255),
                                                                        `status` varchar(50) DEFAULT 'pending',
                                                                        PRIMARY KEY (`Id`),
                                                                        UNIQUE KEY `pro_id` (`product_id`)
                                                                        ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"""
            spider.cursor.execute(create_table)
        except Exception as e:
            spider.logger.info(e)

        try:
            create_table = f"""CREATE TABLE IF NOT EXISTS `{db.db_data_table}` (`Id` INT NOT NULL AUTO_INCREMENT,
                                                                      `Platform_Name` varchar(255),
                                                                      `Platform_URL` varchar(255),
                                                                      `Product_id` varchar(255),
                                                                      `Category` varchar(255),
                                                                      `Sub_Category` varchar(255) DEFAULT 'NA',
                                                                      `Sector` varchar(255) DEFAULT 'NA',
                                                                      `Sub_Sector` varchar(255) DEFAULT 'NA',
                                                                      `SKU_Name` varchar(255),
                                                                      `Manufacturer` varchar(255) DEFAULT 'NA',
                                                                      `Brand` varchar(255),
                                                                      `Sub_Brand` varchar(255) DEFAULT 'NA', 
                                                                      `Origin` varchar(255) DEFAULT 'NA',
                                                                      `Major_Region` varchar(255),
                                                                      `country` varchar(255),
                                                                      `Market_Clusters` varchar(255),
                                                                      `Pack_Size_Local` varchar(255),
                                                                      `Pack_Size` varchar(255),
                                                                      `Pack_Size_New` varchar(255),
                                                                      `Price_In_Local` varchar(255),
                                                                      `Price_In_Local_USD` varchar(255),
                                                                      `Price_Range` varchar(255),
                                                                      `Type_of_Promo` varchar(255),
                                                                      `Promo_Price_Local` varchar(255),
                                                                      `Promo_Price_USD` varchar(255),
                                                                      `Price_per_unit_Local` varchar(255),
                                                                      `Price_per_unit_USD` varchar(255),
                                                                      `Price_per_unit_USD_New` varchar(255),
                                                                      `Age_of_Whisky` varchar(255),
                                                                      `Country_of_Origin` varchar(255),
                                                                      `Distillery` varchar(255) DEFAULT 'NA',
                                                                      `Pack_type` varchar(255),
                                                                      `Tasting_Notes` varchar(255) DEFAULT 'NA',
                                                                      `Image_Urls` varchar(255),
                                                                      `ABV` varchar(255),
                                                                      `scrape_date` varchar(255),
                                                                      `Standard_Currency` varchar(255) DEFAULT 'USA',
                                                                       `type` varchar(255),
                                                                        PRIMARY KEY (`Id`),
                                                                        UNIQUE KEY `Product_id` (`Product_id`)
                                                                        ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;"""
            spider.cursor.execute(create_table)
        except Exception as e:
            spider.logger.info(e)

    def process_item(self, item, spider):
        if isinstance(item, MetrobglinkItem):
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
                    spider.cursor.execute(insert_db, tuple(item.values()))
                    spider.con.commit()
                    self.data_insert += 1
                    spider.logger.info(f'Data Inserted...{self.data_insert}')
                except Exception as e:
                    print(e)
            except Exception as e:
                print(e)

        if isinstance(item, MetrobgdataItem):
            # if isinstance(item, FlipkartItem):
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
                    status = "404"

                update = f'update {db.db_links_table} set status="{status}" where Id=%s'
                # print(update)
                spider.cursor.execute(update, (Id))
                spider.con.commit()
                spider.logger.info('Done...')
            except Exception as e:
                spider.logger.error(e)

        return item
