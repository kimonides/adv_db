from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("paconator").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext

# ---------------------------------------------  Transform csv files to parquet files ------------------------------------------------

# df = spark.read.csv('hdfs://master:9000/data/movies.csv', header = False)
# df.repartition(1).write.mode('overwrite').parquet('hdfs://master:9000/data/movies.parquet')

# df = spark.read.csv('hdfs://master:9000/data/ratings.csv', header = False)
# df.repartition(1).write.mode('overwrite').parquet('hdfs://master:9000/data/ratings.parquet')

# df = spark.read.csv('hdfs://master:9000/data/movie_genres.csv', header = False)
# df.repartition(1).write.mode('overwrite').parquet('hdfs://master:9000/data/movie_genres.parquet')

# ---------------------------------------------                 Query 1               ------------------------------------------------


# q1 =    spark.read.parquet('hdfs://master:9000/data/movies.parquet').rdd. \
#         filter(lambda row: False if row._c3 == None else True). \
#         filter(lambda row: True if row._c3.split('-')[0]>='2000' and row._c3.split('-')[0].isdigit() else False ). \
#         filter(lambda row: True if row._c5 > '0' else False ). \
#         map(lambda row: ( row._c3.split('-')[0], (row._c1, row._c5) ) ). \
#         reduceByKey(lambda x, y : x if x[1]>y[1] else y  ). \
#         sortByKey(ascending=False). \
#         take(20)


# for i in q1:
#         print(i)

# ---------------------------------------------                 Query 2               ------------------------------------------------

# spark.read.parquet('hdfs://master:9000/data/ratings.parquet').rdd. \
# q2 =    sc.textFile('hdfs://master:9000/data/ratings.csv'). \
#         map(lambda row:  ( row.split(',')[0]  , (float(row.split(',')[2]), 1 ) )  ). \
#         reduceByKey( lambda x,y : ( x[0]+y[0] , x[1] + y[1] ) ). \
#         map( lambda row : (row[0] ,( row[1][0]/row[1][1]) )     ). \
#         map( lambda row : (None,(1,1)) if row[1]>3 else (None,(0,1)) ). \
#         reduceByKey( lambda x,y : ( x[0] + y[0] , x[1] + y[1] ) ). \
#         map( lambda row: row[1][0]/row[1][1] ). \
#         collect()

# for i in q2:
#         print(i)

# ---------------------------------------------                 Query 3               ------------------------------------------------


ratings =       sc.textFile('hdfs://master:9000/data/ratings.csv'). \
                map(lambda row : (row.split(',')[1] , row.split(',')[2] )  )

moviesGenres =          sc.textFile('hdfs://master:9000/data/movie_genres.csv'). \
                        map(lambda row : (row.split(',')[0] , row.split(',')[1] )  )

movieGenreRatings = ratings.join(moviesGenres)

q3 =    movieGenreRatings. \
        map( lambda row: (row[1][1] , row[1][0] ) ).collect()
for i in q3:
        print(i)

# print(x.collect())



