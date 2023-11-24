from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("构建rdd.py").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # union
    # 和
    # intersection:
    #
    # *union: 并集, 用于计算两个RDD的并集
    # *intersection: 交集, 用于计算两个RDD的交集
    # rdd_init = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1)
    # rdd_init = rdd_init.map(lambda word: (word, 1))
    rdd_init = sc.parallelize([("apple", 3), ("banana", 2), ("apple", 1)], 2)
    print(rdd_init.collect())
    print(rdd_init.getNumPartitions())
    # 4

    a1 = rdd_init.reduceByKey(lambda agg, curr: agg + curr)
    print(a1.collect())
    # 55

    a2 = rdd_init.foldByKey(10, lambda agg, curr: agg + curr)
    print(a2.collect())


    def rdd_add(agg, curr):

        return curr + agg

    def rdd_add01(agg, curr):
        agg = "kklt"
        return str(curr) + str(agg)
    a3 = rdd_init.aggregateByKey(10, rdd_add, rdd_add01)
    print(a3.collect())
    # 105
    # [('apple', 3), ('banana', 2), ('apple', 1)]
    # 2
    # [('banana', 2), ('apple', 4)]
    # [('banana', 12), ('apple', 24)]
    # [('banana', 12), ('apple', '11kklt')]