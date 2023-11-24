import os
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("test01")
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize(["zhangsan", "lisi", "wangwu", "zhaoniu"], 5)
    print(rdd_init.getNumPartitions())
    print(rdd_init.glom().collect())
    rdd_map = rdd_init.map(lambda name: name + " name 10")
    print(rdd_map.collect())
