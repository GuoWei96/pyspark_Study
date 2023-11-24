import os
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("mappart.py").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    rdd_map = rdd_init.map(lambda num: num + 1)
    print(rdd_map.collect())


    def fn1(iter1):
        arr = []
        for num in iter1:
            arr.append(num + 1)
        return arr


    rdd_res = rdd_init.mapPartitions(fn1)
    print(rdd_res.collect())
