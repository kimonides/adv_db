from pyspark.sql import SparkSession

from io import StringIO
import csv


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


spark = SparkSession.builder.appName("paconator").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext

# outputs (movieID ,genre)    
movieGenres =   sc.textFile('hdfs://master:9000/data/movie_genres_reduced.csv'). \
                map(split_complex). \
                map(lambda row:(row[0],row[1])). \
                groupByKey(). \
                map(lambda row: (row[0],tuple(row[1]))). \
                collect()

movieGenresHashTable = {}
for movieGenre in movieGenres:
    movieGenresHashTable[movieGenre[0]] = movieGenre[1]

movieGenresHashTableBroadcast = sc.broadcast(movieGenresHashTable)

def hashJoin(row):
    res = []
    if(row[0] in movieGenresHashTableBroadcast.value):
        for movieGenre in movieGenresHashTableBroadcast.value[row[0]]:
            res.append((row[0],(row[1],movieGenre)))
        return res
    return []

ratings =   sc.textFile('hdfs://master:9000/data/ratings.csv'). \
            map(split_complex). \
            map(lambda row: (row[1], row[0])). \
            flatMap(hashJoin). \
            collect()

for i in ratings:
    print(i)