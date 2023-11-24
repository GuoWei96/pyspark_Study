import pandas as pd

ss = pd.Series([1, 2, 3, 4, 5])
print(ss)
df = pd.read_csv('/workspace/ppands/data/1960-2019全球GDP数据.csv', encoding='gbk')
print(df.head(10))