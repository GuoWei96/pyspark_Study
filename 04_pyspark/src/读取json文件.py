from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
if __name__ == "__main__":
    # 1- 构建Spark SQL对象
    spark = SparkSession.builder.master('local[*]').appName("create_df").getOrCreate()

    # 2- 读取数据
    # 采用text的方式来读取数据, 仅支持产生一列数据, 默认列名为 value, 当然可以通过schema修改列名
    df_init = spark.read \
        .format('json') \
        .load('file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/people.json')

    df_init.printSchema()
    df_init.show()