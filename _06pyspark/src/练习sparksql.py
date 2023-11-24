from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    spark = SparkSession.builder.appName("练习sparksql.py").master("local[*]").config("spark.sql.shuffle.partitions","50") .getOrCreate()
    df_init = spark.read.csv(
        path="file:///workspace/_06pyspark/data",
        sep=",",
        header=True,
        schema="id int, name String, age int"
    )
    df_filter = df_init.dropDuplicates(["id","name"])

    # 输出为csv
    df_filter.write.mode("overwrite").format("csv").option("header",True).option("sep",",").save("file:///tmp/test.csv")

    # 输出为json
    df_filter.write.mode("overwrite").format("json").save("hdfs://node1:8020/tmp/test.json")

    # 输出为文本,文本只能输出单列
    df_filter.select(["name"]).write.mode("overwrite").format("text").save("hdfs://node1:8020/tmp/test.txt")

    # 输出为orc格式文件
    df_filter.write.mode("overwrite").format("orc").save("hdfs://node1:8020/tmp/test.orc")