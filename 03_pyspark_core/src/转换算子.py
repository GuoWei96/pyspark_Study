import os
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("create_add01").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
    rdd_res = rdd_init.groupBy(lambda num: "0" if num % 2 == 0 else "j")
    print(rdd_res.mapValues(tuple).collect())