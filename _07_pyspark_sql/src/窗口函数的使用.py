from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as win


if __name__ == "__main__":
    spark = SparkSession.builder.appName("窗口函数的使用.py").master("local[*]").getOrCreate()
    df_init = spark.read.csv(
        path="/workspace/_07_pyspark_sql/data/窗口函数",
        header=True,
        inferSchema=True
    )
    df_init.show()
    # Spark SQL
    df_init.createTempView("t1")
    spark.sql("""
    select 
        date,
        pv,
        row_number() over(partition by date order by pv desc) as rank1,
        rank() over(partition by date order by pv desc) as rank2,
        dense_rank() over(partition by date order by pv desc) as rank3
        from t1
    """).show()

    # DSL
    df_init.select(
        df_init["date"],
        df_init["pv"],
        F.row_number().over(win.partitionBy("date").orderBy(F.desc("pv"))).alias("rank1"),
        F.rank().over(win.partitionBy("date").orderBy(F.desc("pv"))).alias("rank2"),
        F.dense_rank().over(win.partitionBy("date").orderBy(F.desc("pv"))).alias("rank3")
    ).show()
