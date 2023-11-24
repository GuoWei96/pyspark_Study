import os
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("create_add01").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd_init = sc.textFile("file:///workspace/03_pyspark_core/data/")
    print(rdd_init.collect())
    print(rdd_init.getNumPartitions())
# 读取一个本文件至少两个分区
# 读取多个本地文件每一个文件有一个分区
# 有多少个分区存储到HDFS上就有多少个小文件