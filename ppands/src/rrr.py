import pandas as pd

dict_data1 = [
    (138, "2023-08-15", 4, "饮料", "零食", 13),
    (139, "2023-08-15", 13, "午饭", "午饭", 23),
    (140, "2023-08-15", 14, "晚饭", "晚饭", 33),

]
df1 = pd.DataFrame(data=dict_data1, columns=["id", "日期", "费用", "备注", "分类", "chinese"],
                   index=[i for i in range(3)])
df = pd.read_csv("/workspace/ppands/data/test.csv",encoding="utf-8",index_col=[0] )
print(df['datetime'].info())
# <class 'pandas.core.series.Series'>
# Index: 3 entries, 1 to 3
# Series name: datetime
# Non-Null Count  Dtype
# --------------  -----
# 3 non-null      object
# dtypes: object(1)
# memory usage: 48.0+ bytes
# None
df = pd.read_csv("/workspace/ppands/data/test.csv",encoding="utf-8",index_col=[3], parse_dates=True )

print(df)
print(df.info())
# <class 'pandas.core.series.Series'>
# Index: 3 entries, 1 to 3
# Series name: datetime
# Non-Null Count  Dtype
# --------------  -----
# 3 non-null      datetime64[ns]
# dtypes: datetime64[ns](1)
# memory usage: 48.0 bytes
# None
