import os
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("wordCount")
    sc = SparkContext(conf=conf)
    rdd_init = sc.textFile("hdfs://node1:8020/tmp/word.txt", 3)
    rdd_flatMap = rdd_init.flatMap(lambda word: word.split())
    print(rdd_flatMap.collect())
    rdd_Map = rdd_flatMap.map(lambda word: (word, 1))
    print(rdd_Map.collect())
    rdd_Res = rdd_Map.reduceByKey(lambda agg, curr: agg + curr)
    print(rdd_Res.collect())
