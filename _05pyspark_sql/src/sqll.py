from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
schema =StructType().add("userid",IntegerType(),False).add("movieid", StringType()).add("score", IntegerType(),False).add("datestr", StringType(),False)
if __name__ == "__main__":
    spark = SparkSession.builder.appName("sql").master("local[*]").getOrCreate()
    df_init = spark.read.csv(path="hdfs://node1:8020/tmp/u.data", sep="\t", schema=schema)

    # df_init.show()
    # 查询用户平均分
    def xunqiu1():
        # DSL
        df_init.groupby("userid").agg(F.round(F.avg("score"),2).alias("u_avg")).orderBy(F.desc("u_avg")).limit(10).show()

        # SQL
        df_init.createTempView("t1")
        spark.sql("select userid, round(avg(score),2) as u_avg from t1 group by userid order by u_avg desc limit 10 ").show()
    # xunqiu1()

    # 查询电影平均分
    def xunqiu2():
        #DSL
        df_init.groupby("movieid").agg(F.round(F.avg("score"),2).alias("m_avg")).orderBy(F.desc("m_avg")).limit(10).show()

        #SQL
        df_init.createTempView("t1")
        spark.sql("select movieid, avg(score) as m_avg from t1 group by movieid order by m_avg desc limit 10").show()
    # xunqiu2()

    # 查询高分电影(评分大于3)中打分次数最多的用户并求出此人打分的平均分
    def xunqiu3():
        ### SQL
        # 1 求出高分电影
        df_init.createTempView("t1")
        spark.sql("""
        select movieid, round(avg(score),2) as m_avg from t1 group by movieid having m_avg > 3
        """).createTempView("top_movie")

        # 2 找出高分电影中打分次数最多的用户
        spark.sql("""
        select t1.userid, count(1) as cnt from top_movie join t1 on t1.movieid = top_movie.movieid group by userid order by cnt desc limit 1
        """).createTempView("max_userid")

        # 3 求出此人在所有电影的平均打分
        spark.sql("""
        select t1.userid,round(avg(score),2)  from t1 join max_userid on t1.userid = max_userid.userid group by t1.userid
        """).show()

        ### DSL
        df_where = df_init.groupby("movieid").agg(F.round(F.avg("score"),2).alias("m_avg")).where("m_avg > 3")
        df_who = df_where.join(df_init,"movieid").groupby("userid").agg(F.count("score").alias("cnt")).orderBy(F.desc("cnt")).limit(1).select("userid")
        df_init.where(df_init["userid"] == df_who.first()["userid"]).groupby("userid").agg(F.round(F.avg("score"),2).alias("m_avg")).show()

    # xunqiu3()


    def xuqiu4():
        # SQL
        df_init.createTempView("t1")
        spark.sql("""
        select userid, round(avg(score),2) as s_avg, max(score) as s_max, min(score) as s_min from t1 group by userid
        """).show()

        #DSL
        df_init.groupby('userid').agg(F.round(F.avg("score"), 2).alias("s_avg"),
                                      F.max("score").alias("s_max"),
                                      F.min("score").alias("s_min")
                                      ).show()
    # xuqiu4()

    # 查询被评分100次的电影的平均分的排名TOP100
    def xuqiu5():
        ### SQL
        # 1 找出被评分100次的电影
        df_init.createTempView("t1")
        spark.sql("""
        select movieid, count(1) as cnt, round(avg(score),2) as m_avg  from t1 group by movieid having cnt > 100
        """).createTempView("movieid_100")

        # 排名TOP100的电影
        spark.sql("""
        select * from movieid_100 order by m_avg desc limit 100
        """).show()


        ### DSL
        df_init.groupby("movieid").agg(F.count("score").alias("cnt"),
                                       F.round(F.avg("score"),2).alias("m_avg"),
                                       ).where('cnt > 100').orderBy(F.desc("m_avg")).limit(100).show()

    # xuqiu5()

    # 列出出电影评分平均分1分以及1分一下的电影
    def xuqiu6():
        ### SQL
        df_init.createTempView("t1")
        spark.sql("""
        select movieid, round(avg(score)) as m_avg from t1 group by movieid having m_avg <= 1 order by m_avg desc limit 10
        """).show()

        ### DSL
        df_init.groupby("movieid").agg(
            F.round(F.avg("score")).alias("m_avg")
        ).where("m_avg <= 1").orderBy(F.desc("m_avg")).limit(10).show()

    # xuqiu6()

    # 把电影平均分1 2 3 4 5分的电影数量写下来
    ### SQL
    def xuqiu7():


        df_init.createTempView("t1")
        spark.sql("""
        select movieid, round(avg(score)) as m_avg from t1 group by movieid
    """).createTempView("t2")
        spark.sql("""
    select count(if(m_avg = 1,m_avg,null)) as cnt_1, count(if(m_avg = 2,m_avg,null)) as cnt_2 ,count(if(m_avg = 3,m_avg,null)) as cnt_3 ,count(if(m_avg = 4,m_avg,null)) as cnt_4 ,count(if(m_avg = 5,m_avg,null)) as cnt_5 from t2 
    """).show()
        # +-----+-----+-----+-----+-----+
        # | cnt_1 | cnt_2 | cnt_3 | cnt_4 | cnt_5 |
        # +-----+-----+-----+-----+-----+
        # | 78 | 249 | 803 | 537 | 15 |
        # +-----+-----+-----+-----+-----+
        spark.sql("""
    select count(1) from t2 where m_avg = 1
    """).show()
        spark.sql("""
        select distinct count(movieid) from t2
        """).show() # 1682
        a = 78 + 249 + 803 +537 +15
        print(a)
    xuqiu7()
