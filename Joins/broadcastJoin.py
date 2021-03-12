from pyspark.sql import SparkSession

from io import StringIO
import csv
import hashlib

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


spark = SparkSession.builder.appName("paconator").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext
p = 5

movieGenresCount =   sc.textFile('hdfs://master:9000/data/movie_genres_reduced.csv').count()
movieGenresCount = sc.broadcast(movieGenresCount)

# movieGenres =   sc.textFile('hdfs://master:9000/data/movie_genres_reduced.csv'). \
#                 map(split_complex). \
#                 map(lambda row:(row[0],row[1]))
#                 # collect()

# def partition(lst, n):
#     """Yield successive n-sized chunks from lst."""
#     for i in range(0, len(lst), n):
#         yield lst[i:i + n]
        

# movieGenres = sc.broadcast(list(partition(movieGenres, p)))

# def hashJoin(partition,):
#     partition = list(partition)
#     partitionSize = len(partition)
#     rightSmaller = False
#     if(movieGenresCount.value<partitionSize):
#         rightSmaller = True
#         hashTable = {}
#         for lst in movieGenres.value:
#             for t in lst:
#                 if t[0] in hashTable:
#                     hashTable[t[0]].append(t[1])
#                 else:
#                     hashTable[t[0]] = [t[1]]
#     else:
#         hashTables = [{} for _ in partition]
#     # Map phase
#     if(rightSmaller):
#         for t in partition:
#             lk = t[0]
#             lv = t[1]
#             if lk in hashTable:
#                 for rv in hashTable[lk]:
#                     yield (lk,(lv,rv))
#     # Close phase
#     else:
#         for t in partition:
#             lk = t[0]
#             lv = t[1]
#             key_hash = int(lk) % p
#             if lk in hashTables[key_hash]:
#                 hashTables[key_hash][lk].append(lv)
#             else:
#                 hashTables[key_hash][lk] = [lv]
#         for i,table in enumerate(hashTables):
#             if table:
#                 Ri = movieGenres.value[i]
#                 for r in Ri:
#                     rk,rv = r
#                     if rk in table:
#                         for lv in table[rk]:
#                             yield ( rk,(rv,lv) ) 
    


# x =   sc.textFile('hdfs://master:9000/data/ratings.csv'). \
#             map(split_complex). \
#             map(lambda row: (row[1], row[0])). \
#             mapPartitions(hashJoin).take(100)

# for i in x:
#     print(i)

def broadcastHashJoin(L,R,p):
    def partition(lst, n):
        """Yield successive n-sized partitions from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    def hashJoin(lPartition):
        lPartition = list(lPartition)
        lPartitionSize = len(lPartition)
        rSize = len(R.value)
        rightSmaller = False
        if(rSize<lPartitionSize):
            rightSmaller = True
            hashTable = {}
            for lst in R.value:
                for t in lst:
                    if t[0] in hashTable:
                        hashTable[t[0]].append(t[1])
                    else:
                        hashTable[t[0]] = [t[1]]
        else:
            hashTables = [{} for _ in lPartition]
        # Map phase
        if(rightSmaller):
            for t in lPartition:
                lk = t[0]
                lv = t[1]
                if lk in hashTable:
                    for rv in hashTable[lk]:
                        yield (lk,(lv,rv))
        # Close phase
        else:
            for t in lPartition:
                lk = t[0]
                lv = t[1]
                key_hash = int(lk) % p
                if lk in hashTables[key_hash]:
                    hashTables[key_hash][lk].append(lv)
                else:
                    hashTables[key_hash][lk] = [lv]
            for i,table in enumerate(hashTables):
                if table:
                    Ri = R.value[i]
                    for r in Ri:
                        rk,rv = r
                        if rk in table:
                            for lv in table[rk]:
                                yield ( rk,(rv,lv) ) 
    #-------------------------- Function Starts Here ------------------------------------
    R = sc.broadcast(list(partition(R.collect(), p)))
    join =  L.mapPartitions(hashJoin)
    return join

# m = sc.parallelize([(1, 2), (3, 4),(1,3)]).groupByKey().map(lambda row:(row[0],tuple(row[1]))).collectAsMap()
# print(m)
movieGenres =   sc.textFile('hdfs://master:9000/data/movie_genres_reduced.csv'). \
                map(split_complex). \
                map(lambda row:(row[0],row[1]))
ratings =   sc.textFile('hdfs://master:9000/data/ratings.csv'). \
            map(split_complex). \
            map(lambda row: (row[1], row[0]))

import time
startTime = time.time()
join = broadcastHashJoin(L=ratings,R=movieGenres,p=1)
join.collect()
endTime = time.time()
print(endTime -startTime)