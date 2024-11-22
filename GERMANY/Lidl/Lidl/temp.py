import re

a='Wild Burrow Gin Entdeckerpaket 2 x 0,7l-Flasche'
b= re.findall(r'([-+]?(?:\d*[\.|\,]*\d+))\s*X\s*([-+]?(?:\d*[\.|\,]*\d+))\s*[-]?\s*([a-z]+)',item['SKU_Name'].replace(',', '.'), flags=re.IGNORECASE)
print(b)