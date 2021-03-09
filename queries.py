#!/usr/bin/env python3.7

from pyspark.sql import SparkSession
from pyspark.sql import Row

from io import StringIO
import csv




################# SQL queries 1-5 #################

#Q_1

spark = SparkSession.builder.appName("ADBMS Project").getOrCreate()
movies = spark.read.format("csv").options(header='false',inferSchema='true').load("hdfs://master:9000/dbms_project/movies.csv")
movies.registerTempTable("movies")
sql_Query1 = """ Select Year(_c3) as year, (_c1) as Title, ((_c6-_c5)/(_c5))*100 as profit  from movies \
    INNER JOIN ( select Year(_c3) as year,MAX(((_c6-_c5)/(_c5))*100) as profit from movies \
    where (_c3) is NOT  NULL and YEAR(_c3) >= 2000 and (_c5) <> 0 and (_c6) <> 0 \
    group by YEAR(_C3)) MAXPROFIT ON MAXPROFIT.year = Year(movies._c3) and MAXPROFIT.profit = (((movies._c6-movies._c5)/movies._c5)*100) order by year desc"""
sqlDF  = spark.sql(sql_Query1)
sqlDF.show()

#Q_2

spark = SparkSession.builder.appName("ADBMS Project").getOrCreate()
ratings = spark.read.format("csv").options(header='false',inferSchema='true').load("hdfs://master:9000/dbms_project/ratings.csv")
ratings.registerTempTable("ratings")
sql_Query2 = """ select (y.us_number*100)/x.tot_us as percentage from (select count(distinct ratings._c0) as tot_us from ratings) x cross join (select count(*) as us_number from (select (_c0) as user_id, AVG(_c2) as avg_score from ratings group by (_c0) ORDER BY (_c0) asc) as temp_res where temp_res.avg_score > 3.0 ) y"""
sqlDF  = spark.sql(sql_Query2)
sqlDF.show()



#Q_3

genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/dbms_project/movie_genres.csv")
genres.registerTempTable('genres')

ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/dbms_project/ratings.csv")
ratings.registerTempTable('ratings')


sqlQuery_3 = """
    select genres._c1 as Genre , avg(t2.rating) as Rating, count( DISTINCT genres._c0) as NumberOfMovies
    from genres 
    inner join (
        select _c1 as movieID,avg(_c2) as rating 
        from ratings group by _c1
    ) t2 on genres._c0=t2.movieID
    group by genres._c1
"""
res = spark.sql(sqlQuery_3)
res.show()

#Q_4


genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/data/movie_genres.csv")
genres.registerTempTable('genres')

movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/data/movies.csv")
movies.registerTempTable('movies')

## This implementation is based on the word count implement one with word count in rdd
 sqlQuery_4_1 = """
     select (YEAR(movies._c3) DIV 5)*5 as Quinquennium, avg(LENGTH(movies._c2) - LENGTH(replace(movies._c2, ' ', ''))+1) as Length from movies
     inner join genres on movies._c0=genres._c0
     where YEAR(movies._c3)>2000 and genres._c1='Drama'
     group by YEAR(movies._c3) DIV 5
     order by YEAR(movies._c3) DIV 5 asc
 """

res = spark.sql(sqlQuery_4_1)

res.show()
## This implementation counts the len of the string description

sqlQuery_4_2 = """
    select YEAR(movies._c3) DIV 5 as Quinquennium, avg(LENGTH(movies._c2)) as Length from movies
    inner join genres on movies._c0=genres._c0
    where YEAR(movies._c3)>2000 and genres._c1='Drama'
    group by YEAR(movies._c3) DIV 5
    order by YEAR(movies._c3) DIV 5 asc
"""


res = spark.sql(sqlQuery_4_2)
res.show()