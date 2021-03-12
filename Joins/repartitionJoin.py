from pyspark.sql import SparkSession
from io import StringIO
import csv


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


spark = SparkSession.builder.appName("paconator").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext

# outputs (movieID,userID)
ratings =   sc.textFile('hdfs://master:9000/data/ratings.csv'). \
            map(split_complex). \
            map(lambda row: (row[1], (row[0])))

# outputs (movieID ,genre)    
movieGenres =   sc.textFile('hdfs://master:9000/data/movie_genres_reduced.csv'). \
                map(split_complex). \
                map(lambda row:(row[0],row[1]))

def repartitionJoin(rdd, other):
    def dispatch(seq):
        LB, RB = [], []
        for (n, v) in seq:
            if n == 'L':
                LB.append(v)
            elif n == 'R':
                RB.append(v)
        return ((l, b) for l in LB for b in RB)
    ls = rdd.mapValues(lambda v: ('L', v))
    rs = other.mapValues(lambda v: ('R', v))
    return ls.union(rs).groupByKey().flatMapValues(lambda x: dispatch(x.__iter__()))

import time

startTime = time.time()
join = repartitionJoin(ratings,movieGenres)
endTime = time.time()
print(endTime -startTime)