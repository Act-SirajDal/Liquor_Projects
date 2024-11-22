import os
import time
from scrapy.cmdline import execute
from amazon_jp.items import *
import pymysql
import amazon_jp.db_config as db
import hashlib

def create_md5_hash(input_string):
    md5_hash = hashlib.md5()
    md5_hash.update(input_string.encode('utf-8'))
    return md5_hash.hexdigest()

class LinkSpider(scrapy.Spider):
    name = 'link'
    cookies = {
        'session-id': '357-2832237-5707069',
        'session-id-time': '2082787201l',
        'i18n-prefs': 'JPY',
        'lc-acbjp': 'en_US',
        'ubid-acbjp': '355-5227062-8480525',
        'session-token': '"c8BAnc+psyY1FkPakauLT2+rKDHiI02fI38MIbCoCuro/1SMKJ+ZaXZh5PL/I7aqe0YgrdiaUuUh7hxOENuVyTaVpGaKnI5lojeLA60SS24W+hpYi2l9XDOIHkIhv1N3pl9LYkgb5Plt6V8XD25R8jXRc3FXEJUoT4TCmvcy4pJB1ABLg0G5CZSrjhtc1ynnaGqj2mfxvK+3VrkU1TMAFc6TpXKwz/fN2uvLARdm/mshVay/B2y5brZpxmXIrBYLizexExbVxqRHWy2yHZPNgbnRHUV8S0XJtht6TZZ3/jGTomlPtz3nGPNwBnRUej8EVEgGJvcAZnBN11HJr22OAPwxk/7kwP5owjtlq9Qa7zI="',
        'csm-hit': 'tb:5FPGDY951QYCWQYAS39G+sa-5FPGDY951QYCWQYAS39G-PSHZXCKQ5P4YAW7D71V8|1699004020174&t:1699004020174&adb:adblk_no',
    }

    headers = {
        'authority': 'www.amazon.co.jp',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        # 'cookie': 'session-id=357-2832237-5707069; session-id-time=2082787201l; i18n-prefs=JPY; lc-acbjp=en_US; ubid-acbjp=355-5227062-8480525; session-token="c8BAnc+psyY1FkPakauLT2+rKDHiI02fI38MIbCoCuro/1SMKJ+ZaXZh5PL/I7aqe0YgrdiaUuUh7hxOENuVyTaVpGaKnI5lojeLA60SS24W+hpYi2l9XDOIHkIhv1N3pl9LYkgb5Plt6V8XD25R8jXRc3FXEJUoT4TCmvcy4pJB1ABLg0G5CZSrjhtc1ynnaGqj2mfxvK+3VrkU1TMAFc6TpXKwz/fN2uvLARdm/mshVay/B2y5brZpxmXIrBYLizexExbVxqRHWy2yHZPNgbnRHUV8S0XJtht6TZZ3/jGTomlPtz3nGPNwBnRUej8EVEgGJvcAZnBN11HJr22OAPwxk/7kwP5owjtlq9Qa7zI="; csm-hit=tb:5FPGDY951QYCWQYAS39G+sa-5FPGDY951QYCWQYAS39G-PSHZXCKQ5P4YAW7D71V8|1699004020174&t:1699004020174&adb:adblk_no',
        'device-memory': '8',
        'downlink': '1.3',
        'dpr': '1',
        'ect': '3g',
        'rtt': '250',
        'sec-ch-device-memory': '8',
        'sec-ch-dpr': '1',
        'sec-ch-ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"15.0.0"',
        'sec-ch-viewport-width': '1360',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        'viewport-width': '1360',
    }

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

        # start_urls_japans = ["https://www.amazon.co.jp//-/en/s?i=food-beverage&rh=n%3A57239051%2Cn%3A71588051%2Cn%3A71625051%2Cn%3A71626051&dc&fs=true&language=en&qid=1693817334&rnid=57239051&ref=sr_nr_n_1&ds=v1%3AVq2RvK4AHAJ%2BbP8Hfqx2OIKRa0bhynYPq3j3hDOXIXM",
        #                     "https://www.amazon.co.jp//-/en/s?i=food-beverage&rh=n%3A57239051%2Cn%3A71588051%2Cn%3A71625051%2Cn%3A71631051&dc&fs=true&language=en&qid=1693817334&rnid=57239051&ref=sr_nr_n_2&ds=v1%3ASp7v0lReJg8eBLLK8WJdwyR4xBGv3KA9UiJ6lWO%2FQfc",
        #                     "https://www.amazon.co.jp//-/en/s?i=food-beverage&rh=n%3A57239051%2Cn%3A71588051%2Cn%3A71625051%2Cn%3A71632051&dc&fs=true&language=en&qid=1693817334&rnid=57239051&ref=sr_nr_n_3&ds=v1%3AAKObxbSaj8HY9QX3S%2FYuCvoDz7F%2BY5cZNy9GV2YaW6Y",
        #                     "https://www.amazon.co.jp//-/en/s?i=food-beverage&rh=n%3A57239051%2Cn%3A71588051%2Cn%3A71625051%2Cn%3A71633051&dc&fs=true&language=en&qid=1693817334&rnid=57239051&ref=sr_nr_n_4&ds=v1%3AvLhGru3YVytb6jkdkZlxB8LtiNZr4KLF8GKr6%2B6cs5U",
        #                     "https://www.amazon.co.jp//-/en/s?i=food-beverage&rh=n%3A57239051%2Cn%3A71588051%2Cn%3A71625051%2Cn%3A71634051&dc&fs=true&language=en&qid=1693817334&rnid=57239051&ref=sr_nr_n_5&ds=v1%3AATrN8z1fPT93atVb8l5us2b4kWe4cglH2uWKY8NPgp4",
        #                     "https://www.amazon.co.jp//-/en/s?i=food-beverage&rh=n%3A57239051%2Cn%3A71588051%2Cn%3A71625051%2Cn%3A71635051&dc&fs=true&language=en&qid=1693817334&rnid=57239051&ref=sr_nr_n_6&ds=v1%3A0AisBQhNUQwZayv1h5ZWMrEXc4rV6LvC7JTIseG1PWo",
        #                     "https://www.amazon.co.jp//-/en/s?i=food-beverage&rh=n%3A57239051%2Cn%3A71588051%2Cn%3A71625051%2Cn%3A2422288051&dc&fs=true&language=en&qid=1693817334&rnid=57239051&ref=sr_nr_n_7&ds=v1%3Aqj4kFxD4ioAeW00RLSNuZrQwlOkJCXzlMzQFeimLi48",
        #                     "https://www.amazon.co.jp//-/en/s?i=food-beverage&rh=n%3A57239051%2Cn%3A71588051%2Cn%3A71625051%2Cn%3A71645051&dc&fs=true&language=en&qid=1693817334&rnid=57239051&ref=sr_nr_n_8&ds=v1%3ARNoq%2FfqvLjpJG6H7EMLJ0NS7yDbYCZqwpjSLqH%2BJLqM",
        #                     "https://www.amazon.co.jp//-/en/s?i=food-beverage&rh=n%3A57239051%2Cn%3A71588051%2Cn%3A71625051%2Cn%3A71639051&dc&fs=true&language=en&qid=1693817334&rnid=57239051&ref=sr_nr_n_9&ds=v1%3A34joI9qXkyN1Q5SFfH4jarqLlNopvIitt8ftp%2F74uuI",
        #                     "https://www.amazon.co.jp//-/en/s?i=food-beverage&rh=n%3A57239051%2Cn%3A71588051%2Cn%3A71625051%2Cn%3A71641051&dc&fs=true&language=en&qid=1693817334&rnid=57239051&ref=sr_nr_n_10&ds=v1%3ABdIri32h1E1XtBCrqvDELpfIZ4Gh%2BweBCqWe%2FBsMBns"
        #                     ]
        # start_urls_japans=["https://www.amazon.co.jp/-/en/b/?node=71626051&ref_=Oct_d_odnav_d_71625051_0&pd_rd_w=DqzsD&content-id=amzn1.sym.d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_p=d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_r=BTE8T4J2EG864J1TJ1YD&pd_rd_wg=6TY47&pd_rd_r=34c6010b-a5f4-4775-bfaa-dd7dc2a2f049",
        #                     "https://www.amazon.co.jp/-/en/b/?node=71645051&ref_=Oct_d_odnav_d_71625051_1&pd_rd_w=DqzsD&content-id=amzn1.sym.d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_p=d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_r=BTE8T4J2EG864J1TJ1YD&pd_rd_wg=6TY47&pd_rd_r=34c6010b-a5f4-4775-bfaa-dd7dc2a2f049"
        #                     "https://www.amazon.co.jp/-/en/b/?node=71631051&ref_=Oct_d_odnav_d_71625051_2&pd_rd_w=DqzsD&content-id=amzn1.sym.d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_p=d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_r=BTE8T4J2EG864J1TJ1YD&pd_rd_wg=6TY47&pd_rd_r=34c6010b-a5f4-4775-bfaa-dd7dc2a2f049",
        #                     "https://www.amazon.co.jp/-/en/b/?node=71634051&ref_=Oct_d_odnav_d_71625051_3&pd_rd_w=DqzsD&content-id=amzn1.sym.d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_p=d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_r=BTE8T4J2EG864J1TJ1YD&pd_rd_wg=6TY47&pd_rd_r=34c6010b-a5f4-4775-bfaa-dd7dc2a2f049",
        #                     "https://www.amazon.co.jp/-/en/b/?node=71632051&ref_=Oct_d_odnav_d_71625051_4&pd_rd_w=DqzsD&content-id=amzn1.sym.d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_p=d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_r=BTE8T4J2EG864J1TJ1YD&pd_rd_wg=6TY47&pd_rd_r=34c6010b-a5f4-4775-bfaa-dd7dc2a2f049",
        #                     "https://www.amazon.co.jp/s?i=food-beverage&bbn=71633051&rh=n%3A57239051%2Cn%3A%2157240051%2Cn%3A71588051%2Cn%3A71625051%2Cn%3A71633051%2Cp_n_location_browse-bin%3A2507870051&language=en&pf_rd_i=71633051&pf_rd_m=AN1VRQENFRJN5&pf_rd_p=be4b0af0-4ea0-46b9-b16b-43f41c3e4e9d&pf_rd_r=9TE24TN1EB07JM8XQJXZ&pf_rd_s=merchandised-search-3&pf_rd_t=101&ref=s9_acss_bw_cg_Vodka_md1_w",
        #                     "https://www.amazon.co.jp/-/en/b/?node=71635051&ref_=Oct_d_odnav_d_71625051_6&pd_rd_w=DqzsD&content-id=amzn1.sym.d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_p=d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_r=BTE8T4J2EG864J1TJ1YD&pd_rd_wg=6TY47&pd_rd_r=34c6010b-a5f4-4775-bfaa-dd7dc2a2f049",
        #                     "https://www.amazon.co.jp/-/en/b/?node=71641051&ref_=Oct_d_odnav_d_71625051_7&pd_rd_w=DqzsD&content-id=amzn1.sym.d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_p=d5212b0f-b3e5-40ff-8146-ae9d18121983&pf_rd_r=BTE8T4J2EG864J1TJ1YD&pd_rd_wg=6TY47&pd_rd_r=34c6010b-a5f4-4775-bfaa-dd7dc2a2f049",
        #                     "https://www.amazon.co.jp/-/en/b/ref=dp_bc_aui_C_3?ie=UTF8&node=2422292051"]

        start_urls_japans=[
                           "https://www.amazon.co.jp/s?rh=n%3A71626051&fs=true&language=en&ref=lp_71626051_sar",
                           "https://www.amazon.co.jp/s?rh=n%3A71635051&fs=true&language=en&ref=lp_71635051_sar",
                           "https://www.amazon.co.jp/s?rh=n%3A71645051&fs=true&language=en&ref=lp_71645051_sar",
                           "https://www.amazon.co.jp/s?rh=n%3A71631051&fs=true&language=en&ref=lp_71631051_sar",
                           "https://www.amazon.co.jp/s?rh=n%3A71634051&fs=true&language=en&ref=lp_71634051_sar",
                           "https://www.amazon.co.jp/s?rh=n%3A71632051&fs=true&language=en&ref=lp_71632051_sar",
                           "https://www.amazon.co.jp/s?rh=n%3A71633051&fs=true&language=en&ref=lp_71633051_sar",
                           "https://www.amazon.co.jp/s?rh=n%3A71641051&fs=true&language=en&ref=lp_71641051_sar",
                           "https://www.amazon.co.jp/s?rh=n%3A2422292051&fs=true&language=en&ref=lp_2422292051_sar"
                           ]

        for start_urls_japan in start_urls_japans:
            hashid = create_md5_hash(f'{start_urls_japan}')
            current_page = 1
            pl_file_path = fr"{db.PAGESAVE}/Amazon_jp_PL_{current_page}_{hashid}.html"
            meta_dict = {"pl_file_path": pl_file_path, "count": 0, "offset": 0, "current_page": current_page, "hashid": hashid}
            if os.path.exists(pl_file_path):
                print('file:///' + pl_file_path)
                yield scrapy.Request(url='file:///' + pl_file_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
            else:
                yield scrapy.Request(url=start_urls_japan,headers=self.headers,cookies=self.cookies,cb_kwargs=meta_dict,dont_filter=True,meta={'proxy': self.proxy})

    def parse(self, response,**kwargs):
        item = AmazonJpLinks()
        current_page = kwargs['current_page']
        hashid = kwargs['hashid']
        pl_file_path = fr"{db.PAGESAVE}/Amazon_jp_PL_{current_page}_{hashid}.html"
        product_id = response.xpath('//div[contains(@data-component-type,"s-search-result")]')
        if product_id:
            if not os.path.exists(pl_file_path):
                product_id_name = f"Amazon_jp_PL_{current_page}_{hashid}"
                db.pagesave(response, product_id_name)
            else:
                print("File Already Available...")

            for id in product_id:
                ID = id.xpath('./@data-asin').get()
                url_japan = f'https://www.amazon.co.jp/en/dp/{ID}'
                item['url'] = url_japan
                item['product_id'] = ID
                yield item
            time.sleep(2)

            next_page = response.xpath('//a[@class="s-pagination-item s-pagination-next s-pagination-button s-pagination-separator"]/@href').get()
            if next_page:
                current_page += 1
                pl_file_path = fr"{db.PAGESAVE}/Amazon_jp_PL_{current_page}_{hashid}.html"
                start_urls_japan = f"https://www.amazon.co.jp/{next_page}"
                meta_dict = {"current_page": current_page, "hashid": hashid}
                if os.path.exists(pl_file_path):
                    print('file:///' + pl_file_path)
                    yield scrapy.Request(url='file:///' + pl_file_path, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True)
                else:
                    yield scrapy.Request(url=start_urls_japan, headers=self.headers, cb_kwargs=meta_dict, callback=self.parse,dont_filter=True,meta={'proxy': self.proxy})


if __name__ == '__main__':
    execute("scrapy crawl link".split())
