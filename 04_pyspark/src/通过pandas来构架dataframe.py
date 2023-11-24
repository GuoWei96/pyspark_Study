from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import pandas as pd
if __name__ == "__main__":
    # 1- 构建Spark SQL对象
    spark = SparkSession.builder.master('local[*]').appName("create_df").getOrCreate()

    # 2- 构建 pandas DF对象
    pd_df = pd.DataFrame({'id': ['c01', 'c02', 'c03'], 'name': ['张三', '李四', '王五']})

    # 3- 将pd_df 转换为 spark SQL的df
    df_init = spark.createDataFrame(pd_df)
    df_init.show()
    df_init.printSchema()
    # +---+----+
    # | id | name |
    # +---+----+
    # | c01 | 张三 |
    # | c02 | 李四 |
    # | c03 | 王五 |
    # +---+----+
    #
    # root
    # | -- id: string(nullable=true)
    # | -- name: string(nullable=true)