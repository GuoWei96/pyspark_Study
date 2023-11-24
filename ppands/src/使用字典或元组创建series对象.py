import pandas as pd

# 使用元组
tup1 = (1, 2, 3, 4)
s1 = pd.Series(tup1, index=("a", "b", "c", "d"))
print(s1)
# a    1
# b    2
# c    3
# d    4
# dtype: int64

# 使用字典
dict1 = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6}
s2 = pd.Series(dict1)
print(s2)
# A    1
# B    2
# C    3
# D    4
# E    5
# F    6
# dtype: int64


# #使用元组
# tst = (1,2,3,4,5,6)
# pd.Series(tst)
#
# #使用字典：
# dst = {'A':1,'B':2,'C':3,'D':4,'E':5,'F':6}
# pd.Series(dst)
