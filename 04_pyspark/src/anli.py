from pyspark import SparkContext

# 创建 SparkContext 对象
sc = SparkContext("local", "CombineByKey Example")

# 创建 RDD，其中包含键值对 (key, value)
data = data = [("Alice", 85), ("Bob", 90), ("Alice", 92), ("Bob", 88), ("Charlie", 78), ("Charlie", 80)]

# 将 RDD 转换为 PairRDD 格式
rdd = sc.parallelize(data)
def fn2(acc, value):
    print(acc[0])
    print("---")
    print(value)
    print(acc[1])
    return (acc[0] + value, acc[1] + 1)

# 使用 combineByKey 计算每个键对应的总和和计数
result = rdd.combineByKey(
    lambda value: (value, 1),  # 初始化累加器，初始值为当前值，计数为1
    fn2,  # 合并当前值到累加器，更新总和和计数

    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # 合并不同分区的累加器，更新总和和计数
)

# 计算每个键对应的平均值
averages = result.mapValues(lambda x: x[0] / x[1])

# 打印结果
for key, value in averages.collect():
    print(f"Key: {key}, Average: {value}")

# 关闭 SparkContext
sc.stop()
