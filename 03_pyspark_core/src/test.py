import jieba
a1 ="我爱陈雪红"



text = "我爱自然语言处理"
seg_list = jieba.cut(text)
print(" ".join(seg_list))
for i in seg_list:
    print(i)

