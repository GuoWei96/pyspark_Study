import os
import jieba
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("mappart.py").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd_init = sc.textFile("hdfs://node1:8020/tmp/SogouQ.sample")
    # rdd_init = sc.textFile("hdfs://node1:8020/tmp/access.log")
    # hdfs: // node1: 8020 / tmp / word.txt
    rdd1 = rdd_init.map(lambda words: words.split())
    # rdd1 = rdd_init.map(lambda words: words[2])
    # print(rdd1.collect())
    # rdd2 = rdd_init.map(lambda list1: list[2])
    # print(rdd2.collect()) replace
    rdd2 = rdd1.map(lambda words: words[2][1:-1])


    # rdd3 = rdd2.map(lambda word : word.replace("[", "").replace["]", ""])
    def fn1(word):
        li1 = []
        ja = jieba.cut(word)
        for i in ja:
            li1.append(i)
        return li1


    rdd3 = rdd2.map(lambda word: fn1(word))
    # print(rdd3.collect())
    rdd4 = rdd3.flatMap(lambda word: word)
    # print(rdd4.collect())
    rdd5 = rdd4.map(lambda word: (word, 1))
    rdd5 = rdd5.reduceByKey(lambda agg, curr: agg + curr).map(lambda word: (word[1], word[0])).sortByKey(False)
    print(rdd5.collect())
