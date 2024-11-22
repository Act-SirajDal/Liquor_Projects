import re
a='Highland Park Whiskey 10 years of Ambassadors Choice'
age = re.findall('([-+]?(?:\d*[\,|\.]*\d+)) year', a, flags=re.IGNORECASE)
print(age)