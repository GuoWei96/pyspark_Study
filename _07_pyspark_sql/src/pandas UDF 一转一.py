from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("基于arrow完成pandas DF 与 Spark DF的互转操作").master(
        "local[*]").getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    df_init = spark.createDataFrame([(2, 3), (4, 5), (10, 11), (12, 13)], schema="num1 int, num2 int")
    df_init.show()


    # 需求: 基于pandas的UDF 完成 对 a 和 b列乘积计算
    # 3.1 自定义一个python的函数: 传入series类型, 返回series类型
    @F.pandas_udf(returnType=IntegerType())
    def pd_cj(num1: pd.Series, num2: pd.Series) -> pd.Series:

        # 这里面可以写pandas的相关api
        return num1 * num2


    # 对python函数进行注册

    def_cj_udf = spark.udf.register("pd_cj_1", pd_cj)

    # 使用自定义函数

    # SQL
    df_init.createTempView("tmp1")
    spark.sql("""
    select num1,num2, pd_cj_1(num1,num2) from tmp1
    """).show()

    # DSL
    df_init.select(
        df_init["num1"],
        df_init["num2"],
        def_cj_udf(df_init["num1"], df_init["num2"]),
        pd_cj(df_init["num1"], df_init["num2"])
    ).show()
