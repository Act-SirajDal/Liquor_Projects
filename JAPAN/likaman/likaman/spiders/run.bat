start "PART:1" python -m scrapy crawl data_extractor -a start=1 -a end=10000
start "PART:2" python -m scrapy crawl data_extractor -a start=10001 -a end=20000
start "PART:3" python -m scrapy crawl data_extractor -a start=20001 -a end=30000
start "PART:4" python -m scrapy crawl data_extractor -a start=30001 -a end=40000
start "PART:5" python -m scrapy crawl data_extractor -a start=40001 -a end=50000