from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("构建Sparksession对象").getOrCreate()
    df_init = spark.read.csv(path="file:////workspace/04_pyspark/data/stu.csv",
                             header=True,
                             sep=",",
                             inferSchema=True
                             )
    # 展示数据信息
    df_init.show()
    # 显示数据元数据信息
    df_init.printSchema()
    ### 需求: 将年龄大于20岁的数据获取出来 :  ###

    # TODO1 用DSL的方式实现
    df_where = df_init.where("age > 20")
    df_where.show()

    # TODO2 用sql的方式实现
    df_init.createTempView("t1")
    spark.sql("""
    select * from t1 where age > 20
    """).show()

    # 关闭 spark对象
    spark.stop()