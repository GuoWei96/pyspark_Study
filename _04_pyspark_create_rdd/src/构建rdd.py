from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("构建rdd.py").setMaster("local[4]")
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize(["张三", "李四", "赵六", "田七", "郭五"], 8)
    print(rdd_init.collect())
    # ['张三', '李四', '赵六', '田七', '郭五']
    print(rdd_init.getNumPartitions())
    print(rdd_init.glom().collect())
    rdd_map = rdd_init.map(lambda word: word + "10")
    print(rdd_map.collect())
    print(rdd_map.getNumPartitions())
    print(rdd_map.glom().collect())
