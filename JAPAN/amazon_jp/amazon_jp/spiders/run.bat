taskkill /IM python.exe /F
start "Part:1" python -m scrapy crawl data -a  start=1 -a end=10000
start "Part:2" python -m scrapy crawl data -a  start=1001 -a end=20000
start "Part:3" python -m scrapy crawl data -a  start=20001 -a end=30000
start "Part:4" python -m scrapy crawl data -a  start=30001 -a end=40000
start "Part:5" python -m scrapy crawl data -a  start=40001 -a end=50000