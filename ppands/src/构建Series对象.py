# import pandas as pd

# # 使用默认自增索引
# s2 = pd.Series([1, 2, 3])
# print(s2)
# # 自定义索引
# s3 = pd.Series([1, 2, 3], index=['A', 'B', 'C'])
# s3
#
#
# 结果为:
# 0    1
# 1    2
# 2    3
# dtype: int64
# A    1
# B    2
# C    3
# dtype: int64
import pandas as pd
s1 = pd.Series(["夏慧娟", "陈雪红", "李菲菲"])
print(s1)
# 0    夏慧娟
# 1    陈雪红
# 2    李菲菲
# dtype: object

s2 = pd.Series(["夏慧娟", "陈雪红", "李菲菲"], index=["A", "B", "C"])
print(s2)
# A    夏慧娟
# B    陈雪红
# C    李菲菲
# dtype: object