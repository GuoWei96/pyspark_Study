from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.appName("返回为字典或者元祖的自定义函数.py.py").master("local[*]").getOrCreate()
    df_init = spark.read.csv(
        path="/workspace/_07_pyspark_sql/data/user.csv",
        header=True,
        inferSchema=True
    )
    df_init.show()


    # 单独分离姓和名

    def split_name_list(name: str):
        return (name[0], name[1::])


    def split_name_dict(name: str):
        return {"xing": name[0], "ming": name[1::]}


    # 返回列表的注册方式
    schema = StructType().add("xing", StringType()).add("ming", StringType())
    name_list = spark.udf.register("name_list_udf", split_name_list, schema)

    # 对于返回为字典的方式
    schema = StructType().add("xing", StringType()).add("ming", StringType())
    name_dict = F.udf(split_name_dict, schema)

    # SQL
    df_init.createTempView("t1")
    spark.sql("""
    select id, name, name_list_udf(name)['xing'], name_list_udf(name)['ming']  as ming from t1
    """).show()

    # DSL
    df_init.select(
        df_init["id"],
        df_init["name"],
        df_init["age"],
        name_list(df_init["name"])["xing"].alias("xing"),
        name_list(df_init["name"])["ming"].alias("ming")
    ).show()
