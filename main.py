from pyspark.sql import SparkSession
from pyspark.sql import Row

from io import StringIO
import csv


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


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

# def getEarnings(a,b):
#         print("a is:" + a)
#         print("b is:" + b)
#         a = int(a)
#         b = int(b)
#         return (float(a)-float(b))/float(b)


# q1 =    sc.textFile('hdfs://master:9000/data/movies.csv'). \
#         map(split_complex). \
#         filter(lambda row: False if row[3] == None else True). \
#         filter(lambda row: True if row[3].split('-')[0]>='2000' and row[3].split('-')[0].isdigit() else False ). \
#         filter(lambda row: True if row[6] > '0' and row[5] > '0' else False ). \
#         map(lambda row: ( row[3].split('-')[0], (row[1], getEarnings(row[6],row[5])  ) ) ). \
#         reduceByKey(lambda x, y : x if x[1]>y[1] else y  ). \
#         sortByKey(ascending=False). \
#         take(20)


# for i in q1:
#         print(i)

# ---------------------------------------------                 Query 2               ------------------------------------------------

# q2 =    sc.textFile('hdfs://master:9000/data/ratings.csv'). \
#         map(split_complex). \
#         map(lambda row:  ( row[0]  , (float(row[2]), 1 ) )  ). \
#         reduceByKey( lambda x,y : ( x[0]+y[0] , x[1] + y[1] ) ). \
#         map( lambda row : (row[0] ,( row[1][0]/row[1][1]) )     ). \
#         map( lambda row : (None,(1,1)) if row[1]>3 else (None,(0,1)) ). \
#         reduceByKey( lambda x,y : ( x[0] + y[0] , x[1] + y[1] ) ). \
#         map( lambda row: row[1][0]/row[1][1] ). \
#         collect()

# for i in q2:
#         print(i)

# ---------------------------------------------                 Query 3               ------------------------------------------------


# ratings = sc.textFile('hdfs://master:9000/data/ratings.csv'). \
#     map(split_complex)). \
#     map(lambda row: (row[1], row[2]))

# moviesGenres = sc.textFile('hdfs://master:9000/data/movie_genres.csv'). \
#     map(split_complex). \
#     map(lambda row: (row[0], row[1]))

# movieGenreRatings = ratings.join(moviesGenres)
# # Now we have tuples of ( movieID , (movieRating,movieGenre) )

# q3 = movieGenreRatings. \
#         map(lambda row: (row[0], (float(row[1][0]), row[1][1], 1))). \
#         reduceByKey(lambda x, y: (x[0]+y[0], x[1], x[2]+y[2])). \
#         map( lambda row: ( row[1][1] , (row[1][0]/row[1][2] , 1 )) ). \
#         reduceByKey( lambda x,y:  (x[0]+y[0] , x[1]+y[1])). \
#         map( lambda row: ( row[0] , ( row[1][0]/row[1][1] , row[1][1] ) )).collect()

# for i in q3:
#     print(i)

# ---------------------------------------------                 Query 4               ------------------------------------------------

# moviesGenres = sc.textFile('hdfs://master:9000/data/movie_genres.csv'). \
#         map(split_complex). \
#         filter(lambda row: True if row[1]=='Drama' else False ). \
#         map(lambda row: (row[0], row[1]))

# movies =        sc.textFile('hdfs://master:9000/data/movies.csv'). \
#                 map( split_complex ). \
#                 map( lambda row: ( row[0], (len(row[2]) , row[3].split('-')[0] ) ) ). \
#                 filter( lambda row: True if row[1][1]>'1999' else False ). \
#                 map( lambda row: ( row[0], ( row[1][0], (int(row[1][1]) % 100)//5 ) ) )
                
# GenreLengths = moviesGenres.join(movies)

# q4 =    GenreLengths. \
#         map( lambda row: ( row[1][1][1],(row[1][1][0],1) ) ). \
#         reduceByKey( lambda x,y: (x[0]+y[0] , x[1]+y[1]) ). \
#         map( lambda row: ( row[0] , row[1][0]/row[1][1] ) ).collect()



# for i in q4:
#         print(i)




# ---------------------------------------------                 Query 1 CSV               ------------------------------------------------

def formatYear(timestamp):
        return timestamp.split('-')[0]

movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/data/movies.csv")

movies.registerTempTable('movies')
spark.udf.register("formatter", formatYear)

# sqlQuery = "select YEAR(_c3) as year,MAX( (_c6+_c7)/_c7 ) as income , movies._c2 as movie from movies where YEAR(_c3)>2000 group by YEAR(_c3) order by YEAR(_c3) desc"
# sqlQuery = """ 
#         SELECT year(_c3), _c1, gross
#         FROM movies
#         WHERE year(_c3) IN (
#                 SELECT year(_c3)
#                 FROM movies
#                 WHERE gross IN (
#                         SELECT MAX( (_c6+_c7)/_c7 ) as gross
#                         FROM movies
#                         GROUP BY year(_c3)
#                 )
#         )
# """

sqlQuery = """SELECT year(_c3)
                FROM movies
                WHERE (_c6+_c7)/_c7 IN (
                        SELECT MAX( (_c6+_c7)/_c7 )
                        FROM movies
                        GROUP BY year(_c3)
                )"""
# sqlQuery = "select * from movies"
# sqlQuery = "SELECT EXTRACT(year from `_c3`) FROM movies"

res = spark.sql(sqlQuery)

res.show()





