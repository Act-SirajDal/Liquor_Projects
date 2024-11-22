import os
import pymysql
import time
import db_config as db
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

select_q = f"SELECT product_id,Product_URl FROM {db.db_links_table} WHERE STATUS='Pending'"
db.cur.execute(select_q)
links = db.cur.fetchall()

co = 0
print(len(links))
for i in links:
    id = i[0]
    url = i[1]
    pdp_path = f"{db.PAGESAVE}/{id}.html"
    if os.path.exists(pdp_path):
        print("Page Already Available for ",id)
    else:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        driver = webdriver.Chrome(options=chrome_options)
        # driver = webdriver.Chrome()
        driver.get(url)
        time.sleep(5)
        # error = driver.find_elements(By.XPATH, '//div[@id="main-message"]/h1/span').text
        # print(error)
        if 'This site can’t be reached' in driver.page_source:
            print(f'This site can’t be reached ID {id}')
        # driver.page_source
        else:
            response = driver.page_source
            db.pagesave(response,id)
            print('Done')

    update_q = f"update {db.db_links_table} set status='SAVE' where Product_Id='{id}'"
    db.cur.execute(update_q)
    db.con.commit()
    print(id)

