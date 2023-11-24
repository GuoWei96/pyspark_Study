from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as win
import pandas as pd
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("基于arrow完成pandas DF 与 Spark DF的互转操作").master(
        "local[*]").getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    df_init = spark.createDataFrame([(2, 3), (4, 5), (10, 11), (12, 13)], schema="num1 int, num2 int")
    df_init.show()


    # 3- 处理数据:
    # 需求: pandas的UDAF需求   对 num2列 求和
    # 3.1 创建python的函数:  接收series类型, 输出基本数据类型(标量)

    # 标量: python的基本数据类型
    @F.pandas_udf(returnType=IntegerType())
    def num2_sum(num2: pd.Series) -> int:
        # 这里面可以写pandas的相关api
        return num2.sum()

    # 注册函数
    num2_sum_1_1 = spark.udf.register("num2_sum_1", num2_sum)

    # SQL
    df_init.createTempView("t1")
    spark.sql("""
    select num2_sum_1(num2) from t1
    """).show()

    # DSL
    df_init.select(
        num2_sum_1_1(df_init["num2"]).over(win.partitionBy("num1").orderBy(F.desc("num2"))).alias("num2_sum")

    ).show()

    df_init.select(
        num2_sum_1_1(df_init["num2"])
    ).show()
