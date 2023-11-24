from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("构建rdd.py").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd1 = sc.parallelize([('c01', '大数据一班'), ('c02', '大数据二班'), ('c03', '大数据三班')])
    rdd2 = sc.parallelize(
        [('c01', '张三'), ('c02', '李四'), ('c01', '王五'), ('c04', '赵六'), ('c02', '田七'), ('c01', '周八')])

    # left join
    print(rdd1.leftOuterJoin(rdd2).collect())
    # [('c02', ('大数据二班', '李四')), ('c02', ('大数据二班', '田七')), ('c01', ('大数据一班', '张三')),
    #  ('c01', ('大数据一班', '王五')), ('c01', ('大数据一班', '周八')), ('c03', ('大数据三班', None))]
    print(rdd2.leftOuterJoin(rdd1).collect())
    # [('c04', ('赵六', None)), ('c02', ('李四', '大数据二班')), ('c02', ('田七', '大数据二班')),
    #  ('c01', ('张三', '大数据一班')), ('c01', ('王五', '大数据一班')), ('c01', ('周八', '大数据一班'))]

    # join
    print(rdd1.join(rdd2).collect())
    # [('c02', ('大数据二班', '李四')), ('c02', ('大数据二班', '田七')), ('c01', ('大数据一班', '张三')),
    #  ('c01', ('大数据一班', '王五')), ('c01', ('大数据一班', '周八'))]
    print(rdd2.join(rdd1).collect())
    # [('c02', ('李四', '大数据二班')), ('c02', ('田七', '大数据二班')), ('c01', ('张三', '大数据一班')),
    #  ('c01', ('王五', '大数据一班')), ('c01', ('周八', '大数据一班'))]

    # full join
    print(rdd1.fullOuterJoin(rdd2).collect())
# [('c04', (None, '赵六')), ('c02', ('大数据二班', '李四')), ('c02', ('大数据二班', '田七')),
#  ('c01', ('大数据一班', '张三')), ('c01', ('大数据一班', '王五')), ('c01', ('大数据一班', '周八')),
#  ('c03', ('大数据三班', None))]

    # right join
    # 类似left join
