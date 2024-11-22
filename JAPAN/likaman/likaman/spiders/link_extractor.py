import scrapy
from scrapy.cmdline import execute
import likaman.db_config as db
from likaman.items import LikamanLink
import pymysql
import os


class LinkExtractSpider(scrapy.Spider):
    name = "link_extract"

    def __init__(self, name=None, start=0, end=0, **kwargs):
        super().__init__(name, **kwargs)
        # DATABASE SPECIFIC VALUES
        self.start = int(start)
        self.end = int(end)
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password,database=db.db_name)
        self.cursor = self.con.cursor()
        self.proxy = 'http://9dbe950ef6284a5da9e7749db9f7cbd1:@api.zyte.com:8011'


    def start_requests(self):
        update_query = f'''UPDATE {db.db_links_table} set status="Pending"'''
        try:
            self.cursor.execute(update_query)
            self.con.commit()
        except Exception as e:
            print(e)

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': '__fs_u_t=e48ba5d0-1f78-4eef-96c8-c970832b5934; __fs_c_s=1; ss_tracking_session_id=a69c8087b9494730be4e2e387bcd72ef; _gid=GA1.2.1664610199.1709016177; cma-shopkey=likaman; cma-unique-user-id=61464ade-9597-41e9-b57c-eed369ac26f1; cma-first-session-id=fd183f67-08e7-4411-9f4a-1fd353683a68; cma-first-session-datetime=20240227154314; cma-session-id=fd183f67-08e7-4411-9f4a-1fd353683a68; _rcmdjp_user_id=.likaman-online.com-1780389345; _ga_4S0882CTZ9=GS1.1.1709016176.1.1.1709016532.60.0.0; _ga=GA1.2.2117645589.1709016176; _gat_gtag_UA_201533564_2=1; ph_phc_tnQZyAK9BM0WkS5fdAZAxA1bNnyKUlWSACWzmxDfVrA_posthog=%7B%22distinct_id%22%3A%2218de94d5cd748-0d9f7bf03543d4-26001b51-e1000-18de94d5cd8ac2%22%7D',
            'If-Modified-Since': 'Tue, 27 Feb 2024 06:49:06 GMT',
            # 'Referer': 'https://www.likaman-online.com/c/westernliquor/whisky?page=2&sort=latest',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        urls = [
                "https://www.likaman-online.com/c/westernliquor/whisky?page=1&sort=latest",
                "https://www.likaman-online.com/c/westernliquor/brandy?page=1&sort=latest",
                "https://www.likaman-online.com/c/westernliquor/spirits/tequila?page=1&sort=latest",
                "https://www.likaman-online.com/c/westernliquor/spirits/vodka?page=1&sort=latest",
                "https://www.likaman-online.com/c/westernliquor/spirits/rum?page=1&sort=latest",
                "https://www.likaman-online.com/c/westernliquor/spirits/gin?page=1&sort=latest",
                # "https://www.likaman-online.com/c/westernliquor/spirits?page=1&sort=latest",
                "https://www.likaman-online.com/c/chuhai?page=1&sort=latest"
                ]
        for url in urls:
            category = url.split('?')[0].split('/')[-1]
            current_page = 1
            pl_file_path = fr"{db.PAGESAVE}/Likaman_PL_{current_page}_{category}.html"
            meta_dict = {"pl_file_path": pl_file_path,"current_page": current_page,"category": category}
            if os.path.exists(pl_file_path):
                print('file:///' + pl_file_path)
                yield scrapy.Request(url='file:///' + pl_file_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
            else:
                yield scrapy.Request(url=url,headers=headers, callback=self.parse, cb_kwargs=meta_dict,dont_filter=False)
    def parse(self, response,**kwargs):
        item = LikamanLink()
        category = kwargs['category']
        current_page = kwargs['current_page']
        pl_file_path = fr"{db.PAGESAVE}/Likaman_PL_{current_page}_{category}.html"
        articles = response.xpath('//div[@class="fs-c-productList__list"]/article')
        if articles:
            if not os.path.exists(pl_file_path):
                product_id_name = f"Likaman_PL_{current_page}_{category}"
                db.pagesave(response, product_id_name)
            else:
                print("File Already Available...")

            for article in articles:
                link = article.xpath('./form//h2/a/@href').get()
                # absolt_url  = response.urljoin(link)
                absolt_url  = "https://www.likaman-online.com" + link
                item['url'] = absolt_url
                p_id = absolt_url.split('/')
                item['product_id'] = p_id[-1]
                item['category'] = kwargs['category']
                yield item

            next_page = response.xpath('//div[@class="fs-c-pagination"]/a[@class="fs-c-pagination__item fs-c-pagination__item--next"]/@href').get()
            if next_page:
                current_page += 1
                pl_file_path = fr"{db.PAGESAVE}/Likaman_PL_{current_page}_{category}.html"
                meta_dict = {"current_page": current_page, "category": category}
                next_page_url = "https://www.likaman-online.com" + next_page
                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'max-age=0',
                    'Connection': 'keep-alive',
                    'Cookie': '__fs_u_t=e48ba5d0-1f78-4eef-96c8-c970832b5934; __fs_c_s=1; ss_tracking_session_id=a69c8087b9494730be4e2e387bcd72ef; _gid=GA1.2.1664610199.1709016177; cma-shopkey=likaman; cma-unique-user-id=61464ade-9597-41e9-b57c-eed369ac26f1; cma-first-session-id=fd183f67-08e7-4411-9f4a-1fd353683a68; cma-first-session-datetime=20240227154314; cma-session-id=fd183f67-08e7-4411-9f4a-1fd353683a68; _rcmdjp_user_id=.likaman-online.com-1780389345; _rcmdjp_deleted_old_history_view=1; _rcmdjp_history_view=519192%2c519336; _ga_4S0882CTZ9=GS1.1.1709016176.1.1.1709018346.59.0.0; _ga=GA1.2.2117645589.1709016176; _gat_gtag_UA_201533564_2=1; ph_phc_tnQZyAK9BM0WkS5fdAZAxA1bNnyKUlWSACWzmxDfVrA_posthog=%7B%22distinct_id%22%3A%2218de94d5cd748-0d9f7bf03543d4-26001b51-e1000-18de94d5cd8ac2%22%7D',
                    'If-Modified-Since': 'Tue, 27 Feb 2024 07:19:19 GMT',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"'
                }
                if os.path.exists(pl_file_path):
                    print('file:///' + pl_file_path)
                    yield scrapy.Request(url='file:///' + pl_file_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
                else:
                    try:
                        yield scrapy.Request(url=next_page_url,headers=headers,callback=self.parse, cb_kwargs=meta_dict,dont_filter=True)
                    except Exception as e:
                        print(e)


if __name__ == '__main__':
    execute('scrapy crawl link_extract'.split())