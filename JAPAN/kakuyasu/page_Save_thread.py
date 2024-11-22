import os
import pymysql
import time
import db_config as db
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor



# Fetch links from the database
select_q = f"SELECT product_id,Product_URl FROM {db.db_links_table} WHERE STATUS='Pending'"
db.cur.execute(select_q)
links = db.cur.fetchall()

def process_link(link):
    id = link[0]
    url = link[1]
    pdp_path = f"{db.PAGESAVE}/{id}.html"

    # Create a new database connection and cursor for each thread
    connection = pymysql.connect(
        host=db.db_host,
        user=db.db_user,
        password=db.db_password,
        database=db.db_name
    )
    cursor = connection.cursor()

    try:
        if os.path.exists(pdp_path):
            print(f"Page Already Available for {id}")
            # Update the status in the database
            try:
                update_q = f"UPDATE {db.db_links_table} SET status='SAVE' WHERE Product_Id = %s"
                cursor.execute(update_q, (id,))
                print(f"Processed {id}")
            except pymysql.MySQLError as e:
                print(f"Error updating record for Product_Id {id}: {e}")
                connection.rollback()  # Rollback in case of error
        else:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            driver = webdriver.Chrome(options=chrome_options)
            try:
                driver.get(url)
                time.sleep(5)

                if 'This site can’t be reached' in driver.page_source:
                    print(f'This site can’t be reached ID {id}')
                else:
                    response = driver.page_source
                    db.pagesave(response, id)  # Saving the page source in the database
                    print(f'Page saved for {id}')
                    # Update the status in the database
                    update_q = f"UPDATE {db.db_links_table} SET status='SAVE' WHERE Product_Id = %s"
                    cursor.execute(update_q, (id,))
                    print(f"Processed {id}")
            finally:
                driver.quit()

        # Commit transaction after processing the link
        connection.commit()

    except pymysql.MySQLError as e:
        print(f"Database error for Product_Id {id}: {e}")
        connection.rollback()  # Rollback in case of error

    finally:
        # Ensure cursor and connection are closed
        cursor.close()
        connection.close()

# Parallel execution with 10 threads
with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(process_link, links)

print(f"Total links processed: {len(links)}")