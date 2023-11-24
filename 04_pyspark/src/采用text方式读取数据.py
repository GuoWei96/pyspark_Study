from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    # 1. 构建DataFrame对象
    spark = SparkSession.builder.master("local[*]").appName("通过读取外部文件的方式来构建DataFrame.py").getOrCreate()
    df_init = spark.read.format("text").schema(schema='id, String')\
        .load("file:////workspace/04_pyspark/data/stu.text")
    df_init.show()
    df_init.printSchema()