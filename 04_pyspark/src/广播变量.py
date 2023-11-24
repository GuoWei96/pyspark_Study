from pyspark import SparkConf, SparkContext, StorageLevel
import time

if __name__ == "__main__":
    conf = SparkConf().setAppName("演示缓存").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize(
        [('c01', '张三'), ('c02', '李四'), ('c01', '王五'), ('c01', '赵六'), ('c02', '田七'), ('c03', '周八'),
         ('c02', '李九')])
    print(rdd_init.glom().collect())
    print(rdd_init.getNumPartitions())
    def fn1(agg):
        return [agg]

    def fn2(agg, curr):
        agg.append(curr)
        return agg


    def fn3(agg, curr):
        # print(agg)
        # agg.extend(curr)
        return agg
    rdd_res = rdd_init.combineByKey(fn1,fn2,fn3)
    print(rdd_res.collect())