from pyspark import SparkConf, SparkContext, StorageLevel
import time

if __name__ == "__main__":
    conf = SparkConf().setAppName("演示缓存").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setCheckpointDir("/tmp/checkpoint")
    rdd = sc.parallelize([1, 2, 3, 4, 5])
    rdd_res = rdd.map(lambda num: num + 100)
    rdd_res.checkpoint()
    rdd_res.persist(storageLevel=StorageLevel.MEMORY_ONLY)
    rdd_res.count()
    print(rdd_res.collect())
