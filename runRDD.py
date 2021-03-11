from pyspark.sql import SparkSession
from pyspark.sql import Row
from io import StringIO
import csv
import time
import sys,os

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("paconator").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

def q1():
    def getEarnings(a,b):
            print("a is:" + a)
            print("b is:" + b)
            a = int(a)
            b = int(b)
            return (float(a)-float(b))/float(b) * 100

    q1 =    sc.textFile('hdfs://master:9000/data/movies.csv'). \
            map(split_complex). \
            filter(lambda row: False if row[3] == None else True). \
            filter(lambda row: True if row[3].split('-')[0]>='2000' and row[3].split('-')[0].isdigit() else False ). \
            filter(lambda row: True if row[6] > '0' and row[5] > '0' else False ). \
            map(lambda row: ( row[3].split('-')[0], (row[1], getEarnings(row[6],row[5])  ) ) ). \
            reduceByKey(lambda x, y : x if x[1]>y[1] else y  ). \
            sortByKey(ascending=False). \
            take(20)

    for i in q1:
            print(i)
def q2():
    q2 =    sc.textFile('hdfs://master:9000/data/ratings.csv'). \
            map(split_complex). \
            map(lambda row:  ( row[0]  , (float(row[2]), 1 ) )  ). \
            reduceByKey( lambda x,y : ( x[0]+y[0] , x[1] + y[1] ) ). \
            map( lambda row : (row[0] ,( row[1][0]/row[1][1]) )     ). \
            map( lambda row : (None,(1,1)) if row[1]>3 else (None,(0,1)) ). \
            reduceByKey( lambda x,y : ( x[0] + y[0] , x[1] + y[1] ) ). \
            map( lambda row: row[1][0]/row[1][1] ). \
            collect()

    for i in q2:
            print(i)


def q3():
    ratings = sc.textFile('hdfs://master:9000/data/ratings.csv'). \
        map(split_complex). \
        map(lambda row: (row[1], row[2]))

    moviesGenres = sc.textFile('hdfs://master:9000/data/movie_genres.csv'). \
        map(split_complex). \
        map(lambda row: (row[0], row[1]))

    movieGenreRatings = ratings.join(moviesGenres)
    # Now we have tuples of ( movieID , (movieRating,movieGenre) )

    q3 = movieGenreRatings. \
            map(lambda row: (row[0], (float(row[1][0]), row[1][1], 1))). \
            reduceByKey(lambda x, y: (x[0]+y[0], x[1], x[2]+y[2])). \
            map( lambda row: ( row[1][1] , (row[1][0]/row[1][2] , 1 )) ). \
            reduceByKey( lambda x,y:  (x[0]+y[0] , x[1]+y[1])). \
            map( lambda row: ( row[0] , ( row[1][0]/row[1][1] , row[1][1] ) )).collect()

    for i in q3:
        print(i)

def q4():
    moviesGenres = sc.textFile('hdfs://master:9000/data/movie_genres.csv'). \
            map(split_complex). \
            filter(lambda row: True if row[1]=='Drama' else False ). \
            map(lambda row: (row[0], row[1]))

    movies =        sc.textFile('hdfs://master:9000/data/movies.csv'). \
                    map( split_complex ). \
                    map( lambda row: ( row[0], (len(row[2]) , row[3].split('-')[0] ) ) ). \
                    filter( lambda row: True if row[1][1]>'1999' else False ). \
                    map( lambda row: ( row[0], ( row[1][0], (int(row[1][1]) % 100)//5 ) ) )
                    
    GenreLengths = moviesGenres.join(movies)

    q4 =    GenreLengths. \
            map( lambda row: ( row[1][1][1],(row[1][1][0],1) ) ). \
            reduceByKey( lambda x,y: (x[0]+y[0] , x[1]+y[1]) ). \
            map( lambda row: ( row[0] , row[1][0]/row[1][1] ) ).collect()



    for i in q4:
            print(i)

def q5():
    #outputs (movieID,(userID,rating))
    ratings =       sc.textFile('hdfs://master:9000/data/ratings.csv'). \
                    map(split_complex). \
                    map(lambda row: (row[1], (row[0],row[2])))
    # outputs (movieID ,genre)    
    movieGenres =   sc.textFile('hdfs://master:9000/data/movie_genres.csv'). \
                    map(split_complex)
    # outputs (movieID , ( (userID,rating),genre ))
    movieGenreRatings = ratings.join(movieGenres)
    # outputs (movieID, (movieName,moviePopularity))
    movies =        sc.textFile('hdfs://master:9000/data/movies.csv'). \
                    map(split_complex). \
                    map(lambda row : (row[0],(row[1],row[7])))
    # outputs (movieID, ( ((userID,rating),genre),(movieName,moviePopularity) ))
    movieGenreRatings = movieGenreRatings.join(movies)
    def emitUsers(row):
        movieName = row[1][1][0]
        moviePopularity = row[1][1][1]
        userID = row[1][0][0][0]
        rating = row[1][0][0][1]
        genre = row[1][0][1]
        # ( (userID,genre),( (leastLikedMovie,leastLikedMovieRating),(mostLikedMovie,mostLikedMovieRating),1 ) )
        return ( (userID,genre),((rating,movieName,moviePopularity),(rating,movieName,moviePopularity),1) )
    # (leastLikedMovieRating,leastLikedMovie,leastLikedMoviePopularity),(mostLikedMovieRating,mostLikedMovie,mostLikedMoviePopularity),count
    def reduceThisShit(x,y):
        leastLikedMovieRatingX = x[0][0]
        mostLikedMovieRatingX = x[1][0]
        countX = x[2]
        leastLikedMovieRatingY = y[0][0]
        mostLikedMovieRatingY = y[1][0]
        countY = y[2]
        
        mostLiked = x[1] if (mostLikedMovieRatingX>mostLikedMovieRatingY or (mostLikedMovieRatingX==mostLikedMovieRatingY and float(x[1][2])>float(y[1][2])))  else y[1]
        leastLiked = x[0] if (leastLikedMovieRatingX<leastLikedMovieRatingY or (leastLikedMovieRatingX==leastLikedMovieRatingY and float(x[0][2])>float(y[0][2]))) else y[0]

        return ( leastLiked,mostLiked,countX+countY )

    q5 =        movieGenreRatings. \
                map( emitUsers ). \
                reduceByKey(reduceThisShit). \
                map( lambda row: ( row[0][1], ( row[0][0],row[1][0],row[1][1],row[1][2] ) ) ). \
                reduceByKey( lambda x,y: x if x[3]>y[3] else y ). \
                sortByKey(). \
                collect()

    for i in q5:
        print(i)


def benchmark():
    l = [q1,q2,q3,q4,q5]
    times = []
    for (i,f) in enumerate(l):
        spark.catalog.clearCache()
        i+=1
        print('Running Query %s' % i)
        sys.stdout = open('./results_rdd/query_%s'%i, 'w+') 
        startTime = time.time()
        f()
        endTime = time.time()
        sys.stdout = sys.__stdout__
        runTime = endTime -startTime
        times.append(runTime)
    sys.stdout = open('./results_rdd/times', 'w+') 
    for (i,t) in enumerate(times):
        print('Query %s time:%s' % (i,t))
    sys.stdout = sys.__stdout__


if __name__ == '__main__': 
    benchmark()