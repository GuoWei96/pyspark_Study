from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd

if __name__ == "__main__":
    spark = SparkSession.builder.appName("基于arrow完成pandas DF 与 Spark DF的互转操作").master(
        "local[*]").getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # pandas 转 Spark dataframe
    pd_df = pd.DataFrame({"name": ["kaka", "kklt", "guowei"], "age": [13, 45, 34]})

    # 用pandas过滤
    pd_df_filter = pd_df[pd_df["age"] > 20]

    # print(pd_df_filter)

    # 转换为 spark dataframe
    spark_df = spark.createDataFrame(pd_df_filter)
    spark_df.show()

    # 执行spark SQL的api
    spark_df.select(spark_df["name"]).show()

    # 将spark dataframe 转换为pandas的dataframe
    pd_df_fin = spark_df.toPandas()
    print(pd_df_fin)