import os
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("mappart.py").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
    def fn1(list01):
        for i in list01:
            print(i)

    rdd_init.foreach(lambda num: print(num))
    print("--------------")
    rdd_init.foreachPartition(fn1)