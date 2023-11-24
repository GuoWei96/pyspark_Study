from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    # 1. 构建DataFrame对象
    spark = SparkSession.builder.master("local[*]").appName("通过读取外部文件的方式来构建DataFrame.py").getOrCreate()

    # 2- 读取数据
    # sep参数: 设置csv文件中字段的分隔符号,默认为 逗号
    # header参数: 设置csv是否含有头信息  True 有  默认为 false
    # inferSchema参数: 用于让程序自动推断字段的类型  默认为false  默认所有的类型都是string
    # encoding参数: 设置对应文件的字符集 默认为 UTF-8
    df_init = (spark.read.format("csv").option("sep", ",")\
               .option("header", True).option("inferSchema", True)).option("encoding","utf-8")\
        .load("file:////workspace/04_pyspark/data/stu.csv")
    df_init.show()
    df_init.printSchema()
    # +---+----+---+
    # | id | name | age |
    # +---+----+---+
    # | 1 | 张三 | 20 |
    # | 2 | 李四 | 18 |
    # | 3 | 王五 | 22 |
    # | 4 | 赵六 | 25 |
    # +---+----+---+
    #
    # root
    # | -- id: integer(nullable=true)
    # | -- name: string(nullable=true)
    # | -- age: integer(nullable=true)
