from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.appName("窗口函数的使用.py").master("local[*]").getOrCreate()
    df_init = spark.read.csv(
        path="/workspace/_07_pyspark_sql/data/user.csv",
        header=True,
        inferSchema=True
    )
    df_init.show()

    @F.udf(returnType=StringType())
    def name_add_name(name: str) -> str:
        return "name: " + name


    name_add_name_1 = spark.udf.register("name_add_name_2", name_add_name)

    # DSL
    df_init.select(
        "id",
        "name",
        "age",
        name_add_name(df_init["name"]),
        name_add_name(df_init["name"]),
    ).show()

    # SQL
    print("------sql------")
    df_init.createTempView("t1")
    spark.sql("""
    select id,name,age, name_add_name_2(name) from t1
    """).show()