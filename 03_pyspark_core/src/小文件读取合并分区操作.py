import os
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("create_add01").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd_init = sc.wholeTextFiles("file:///workspace/03_pyspark_core/data/")
    print(rdd_init.getNumPartitions())
    print(rdd_init.glom().collect())
    print(rdd_init.collect())
# [('file:/workspace/03_pyspark_core/data/wordcount.txt', 'hadoop hadoop hive hive\nhive java java flink'), ('file:/workspace/03_pyspark_core/data/wordcount.txt01', 'chenpeng guowei chenxuehong')]
