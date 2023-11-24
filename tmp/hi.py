import os
from pyspark import SparkConf, SparkContext

# 程序入口
def use_spark_to_wordcount():
    if __name__ == '__main__':
        print("wordcount")
        # 初始化(Initializing Spark)Spark对象 : SparkText setAppName:当前应用名称, setMaster: 设置启用线程数
        conf = SparkConf().setAppName("wordcount").setMaster("local[*]")
        sc = SparkContext(conf=conf)

        # 1.==========================================
        # 读取文件,获取rdd对象
        # rdd_init = sc.textFile("../data/words.txt")
        # 本地文件用 file:///
        rdd_init = sc.textFile("file:///workspace/tmp/test.txt")
        # 读取hdfs文件  hdfs://192.168.1.100:8020
        # rdd_init = sc.textFile("hdfs://node1:8020:/words.txt")

        # print(rdd_init.collect())
        ## 如果服务器没有安装java可能会报错
        ### 将文本数据按行读取,每一行是列表的一个元素
        ##### ['kaka kklt', 'kaka kklt', 'kaka ka']

        # 2.==================================================
        # 对每行数据执行切割操作, 转换为一个个列表
        # 一对一的转换操作
        rdd_map = rdd_init.map(lambda line: line.split())

        print(rdd_map.collect())
        # 将上一次的列表中的元素按照空格分割,每一个元素转换为一个新的内嵌列表
        # [['kaka', 'kklt'], ['kaka', 'kklt'], ['kaka', 'ka']]

        # 3.==================================================
        # rdd_flatmap = rdd_init.flatMap(lambda line: line.split())
        # print(rdd_flatmap.collect())

       # flatMap类似增强版map,直接将list里面的每个元素按照指定分隔符分割重构list
        # print(rdd_flatmap.collect())
        # ['kaka', 'kklt', 'kaka', 'kklt', 'kaka', 'ka']

        # 4.==================================================
        # # 将每个元素转换为元祖(元素, 1)
        # rdd_map = rdd_flatmap.map(lambda word: (word, 1))
        #
        # # 将list里面的元素lamda为(word, 1)
        # # print(rdd_map.collect())
        # # [('kaka', 1), ('kklt', 1), ('kaka', 1), ('kklt', 1), ('kaka', 1), ('ka', 1)]
        #
        # # 5.==================================================
        # #     根据k分组聚合统计
        # # reduceByKey根据key进行分组,对value进行聚合统计,函数用于设置聚合方案
        # rdd_res = rdd_map.reduceByKey(lambda x, y: x+y)
        # print(rdd_res.collect())
        # # [('kaka', 3), ('kklt', 2), ('ka', 1)]


use_spark_to_wordcount()



# python的处理方法
# def py_wc():
#     with open("../data/words.txt", "r") as f:
#         data = f.read()
#     list1 = data.split("\n")
#     list2 = list()
#     dict1 = dict()
#     for i in list1:
#         list2.append(i.split(" "))
#     for i in list2:
#         for j in i:
#             if not j in dict1:
#                 dict1[j] = 1
#             else:
#                 dict1[j] += 1
#     print(dict1)
#
# py_wc()