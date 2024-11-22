import scrapy


# class ImpersonateSpider(scrapy.Spider):
name = "impersonate_spider"
custom_settings = {
    "DOWNLOAD_HANDLERS": {
        "http": "scrapy_impersonate.ImpersonateDownloadHandler",
        "https": "scrapy_impersonate.ImpersonateDownloadHandler",
    },
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
}

def start_requests(self):
    for browser in ["chrome110", "edge99", "safari15_5"]:
        yield scrapy.Request(
            "https://api.cloud.kaufland.de/search/v1/result-product-offers/?requestType=load&page=1&pageType=category_item_list&categoryId=2431&deviceType=desktop&loadType=pagination&useNewUrls=false&includeExtraAds=false",
            dont_filter=True,
            meta={"impersonate": browser},
        )

def parse(self, response,**kwargs):
    print(response.text)
    # ja3_hash: 773906b0efdefa24a7f2b8eb6985bf37
    # ja3_hash: cd08e31494f9531f560d64c695473da9
    # ja3_hash: 2fe1311860bc318fc7f9196556a2a6b9
    yield {"ja3_hash": response.json()["ja3_hash"]}
