#!/usr/bin/env python3.7

from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
from io import StringIO
import csv

import time
queries = [
    """ 
    Select Year(_c3) as year, (_c1) as Title, ((_c6-_c5)/(_c5))*100 as profit  from movies 
    INNER JOIN ( select Year(_c3) as year,MAX(((_c6-_c5)/(_c5))*100) as profit from movies 
    where (_c3) is NOT  NULL and YEAR(_c3) >= 2000 and (_c5) <> 0 and (_c6) <> 0 
    group by YEAR(_C3)) MAXPROFIT ON MAXPROFIT.year = Year(movies._c3) and 
    MAXPROFIT.profit = (((movies._c6-movies._c5)/movies._c5)*100) order by year desc
    """,
    """ 
    select (y.us_number*100)/x.tot_us as percentage from 
    (select count(distinct ratings._c0) as tot_us from ratings) x 
    cross join (select count(*) as us_number from (select (_c0) as user_id, 
    AVG(_c2) as avg_score from ratings group by (_c0) ORDER BY (_c0) asc) 
    as temp_res where temp_res.avg_score > 3.0 ) y
    """,
    """
    select genres._c1 as Genre , avg(t2.rating) as Rating, count( DISTINCT genres._c0) as NumberOfMovies
    from genres 
    inner join (
        select _c1 as movieID,avg(_c2) as rating 
        from ratings group by _c1
    ) t2 on genres._c0=t2.movieID
    group by genres._c1""",
    """
    select (YEAR(movies._c3) DIV 5)*5 as Quinquennium, avg(LENGTH(movies._c2) - 
    LENGTH(replace(movies._c2, ' ', ''))+1) as Length from movies
    inner join genres on movies._c0=genres._c0
    where YEAR(movies._c3)>2000 and genres._c1='Drama'
    group by YEAR(movies._c3) DIV 5
    order by YEAR(movies._c3) DIV 5 asc
    """,
    """
select temp_1.genre as genre , MAx(temp_0.user_id) as user_id, MAX(temp_1.maxnum) as reviews
from
(select ratings._c0 as user_id, genres._c1 as genre, count(*) as number
 from  ratings inner join genres
 on ratings._c1 = genres._c0
 group by ratings._c0,genres._c1) temp_0
inner join
(select temp.genre as genre, MAX(temp.number) as maxnum
from
(select ratings._c0 as user_id, genres._c1 as genre, count(*) as number
from ratings inner join genres 
on ratings._c1 = genres._c0
group by ratings._c0, genres._c1) temp 
group by temp.genre
order by temp.genre asc) temp_1
on temp_0.genre = temp_1.genre 
and temp_0.number = temp_1.maxnum
group by temp_1.genre
order by temp_1.genre asc
"""
    ]

spark = SparkSession.builder.appName("ADBMS Project").getOrCreate()
movies = spark.read.format("csv").options(header='false',inferSchema='true').load("hdfs://master:9000/data/movies.csv")
movies.registerTempTable("movies")
ratings = spark.read.format("csv").options(header='false',inferSchema='true').load("hdfs://master:9000/data/ratings.csv")
ratings.registerTempTable("ratings")
genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/data/movie_genres.csv")
genres.registerTempTable('genres')


def benchmark():
    times = []
    for (i,q) in enumerate(queries):
        spark.catalog.clearCache()
        i+=1
        print('Running Query %s' % i)
        sys.stdout = open('./results_sql/query_%s'%i, 'w+') 
        startTime = time.time()
        res = spark.sql(q)
        res.show()
        endTime = time.time()
        runTime = endTime -startTime
        times.append(runTime)
        sys.stdout = sys.__stdout__
    sys.stdout = open('./results_sql/times', 'w+') 
    for (i,t) in enumerate(times):
        print('Query %s time:%s' % (i,t))
    sys.stdout = sys.__stdout__


if __name__ == '__main__': 
    benchmark()
