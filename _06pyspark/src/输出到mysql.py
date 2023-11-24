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
    df_filter = df_init.dropDuplicates(["id", "name"])
    df_filter.write.mode("overwrite").format("jdbc")\
        .option("url","jdbc:mysql://node1:3306/tmp?useSSL=false&useUnicode=true&characterEncoding=utf-8")\
        .option("dbtable","tmp_user")\
        .option("user","root")\
        .option("password","root")\
        .save()