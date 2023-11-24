from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("基于arrow完成pandas DF 与 Spark DF的互转操作").master(
        "local[*]").getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    df_init = spark.read.csv(
        path="/workspace/_07_pyspark_sql/data/篮球",
        sep=",",
        header=True,
        inferSchema=True
    )
    df_init.show()
    df_init.printSchema()


    # 必须自定义panda的UDF函数使用(无难度(连一点点都没有))

    def xuqiu1():
        # 思考：助攻这一列需要加10，如何实现？思考使用哪种？
        @F.pandas_udf(returnType=IntegerType())
        def zhugong_add(zhuong: pd.Series) -> pd.Series:
            return zhuong + 10

        zhugong_add_1 = spark.udf.register("zhugong_add_1_1", zhugong_add)

        # SQL

        df_init.createTempView("t1")
        spark.sql("""
        select zhugong_add_1_1(`助攻`) from t1
        """).show()

        # DSL
        df_init.select(
            zhugong_add_1(df_init["`助攻`"])
        ).show()


    # xuqiu1()

    def xuqiu2():
        # ：篮板 + 助攻的次数，思考使用哪种？
        @F.pandas_udf(returnType=IntegerType())
        def zhugong_add(zhuong: pd.Series, lanban: pd.Series) -> pd.Series:
            return zhuong + lanban

        zhugong_add_1 = spark.udf.register("zhugong_add_1_1", zhugong_add)

    # SQL
        df_init.createTempView("t1")
        spark.sql("""
    select `对手`,zhugong_add_1_1(`篮板`,`助攻`) from t1
    """).show()

        # DSL
        df_init.select(
            df_init["`对手`"],
            zhugong_add_1(df_init["`篮板`"], df_init["`助攻`"])

        ).show()

    # xuqiu2()

    def xuqiu3():
        # 统计胜和负的平均分
        @F.pandas_udf(returnType=FloatType())
        def zhugong_add(zhuong: pd.Series) -> float:
            return zhuong.mean()

        zhugong_add_1 = spark.udf.register("zhugong_add_1_1", zhugong_add)

        # SQL
        df_init.createTempView("t1")
        spark.sql("""
        select `胜负`, zhugong_add_1_1(`得分`) from t1 group by `胜负`
        """).show()

        # DSL
        df_init.groupby(df_init["`胜负`"]).agg(
            zhugong_add_1('`得分`').alias("m_avg")
        ).show()

    xuqiu3()

