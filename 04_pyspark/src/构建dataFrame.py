from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("构建dataFrame.py").getOrCreate()

    # 从Spark中获取SparkContext对象
    sc = spark.sparkContext
    rdd_init = sc.parallelize([('c01','张三',20),('c02','李四',15),('c03','王五',26),('c01','赵六',30)])

    # 将RDD转换为dataFrame
    # 方案一:
    schema = StructType().add('id', StringType(), True)\
        .add('name', StringType(), False)\
        .add('age', IntegerType())
    df_init = spark.createDataFrame(rdd_init, schema=schema)
    df_init.printSchema()
    df_init.show()
#     | -- id: string(nullable=true)
#     | -- name: string(nullable=false)
#     | -- age: integer(nullable=true)
#
# +---+----+---+
# | id | name | age |
# +---+----+---+
# | c01 | 张三 | 20 |
# | c02 | 李四 | 15 |
# | c03 | 王五 | 26 |
# | c01 | 赵六 | 30 |
# +---+----+---+

    # 方案二
    df_init = spark.createDataFrame(rdd_init)
    df_init.printSchema()
    df_init.show()
#     | -- _1: string(nullable=true)
#     | -- _2: string(nullable=true)
#     | -- _3: long(nullable=true)
#
# +---+----+---+
# | _1 | _2 | _3 |
# +---+----+---+
# | c01 | 张三 | 20 |
# | c02 | 李四 | 15 |
# | c03 | 王五 | 26 |
# | c01 | 赵六 | 30 |
# +---+----+---+

    # 方案三
    df_init = spark.createDataFrame(rdd_init, schema=['id','name','age'])
    df_init.show()
    df_init.printSchema()
    # +---+----+---+
    # | id | name | age |
    # +---+----+---+
    # | c01 | 张三 | 20 |
    # | c02 | 李四 | 15 |
    # | c03 | 王五 | 26 |
    # | c01 | 赵六 | 30 |
    # +---+----+---+
    #
    # root
    # | -- id: string(nullable=true)
    # | -- name: string(nullable=true)
    # | -- age: long(nullable=true)

    # 方案四
    df_init = rdd_init.toDF()
    df_init.show()
    df_init.printSchema()
    # +---+----+---+
    # | _1 | _2 | _3 |
    # +---+----+---+
    # | c01 | 张三 | 20 |
    # | c02 | 李四 | 15 |
    # | c03 | 王五 | 26 |
    # | c01 | 赵六 | 30 |
    # +---+----+---+
    #
    # root
    # | -- _1: string(nullable=true)
    # | -- _2: string(nullable=true)
    # | -- _3: long(nullable=true)

    # 方案五
    df_init = rdd_init.toDF(schema=schema)
    df_init.show()
    df_init.printSchema()
    # +---+----+---+
    # | id | name | age |
    # +---+----+---+
    # | c01 | 张三 | 20 |
    # | c02 | 李四 | 15 |
    # | c03 | 王五 | 26 |
    # | c01 | 赵六 | 30 |
    # +---+----+---+
    #
    # root
    # | -- id: string(nullable=true)
    # | -- name: string(nullable=false)
    # | -- age: integer(nullable=true)

    # 方案6
    df_init = rdd_init.toDF(schema=['id','name','age'])
    df_init.show()
    df_init.printSchema()
    # +---+----+---+
    # | id | name | age |
    # +---+----+---+
    # | c01 | 张三 | 20 |
    # | c02 | 李四 | 15 |
    # | c03 | 王五 | 26 |
    # | c01 | 赵六 | 30 |
    # +---+----+---+
    #
    # root
    # | -- id: string(nullable=true)
    # | -- name: string(nullable=true)
    # | -- age: long(nullable=true)