#!/usr/bin/env python3.7

from pyspark.sql import SparkSession
from pyspark.sql import Row

from io import StringIO
import csv



#Convert the  csv files to .parquet while maintaining the schema 

spark = SparkSession.builder.appName("ADBMS Project").getOrCreate()
moviesDF = spark.read.csv("hdfs://master:9000/dbms_project/movies.csv")
moviesDF.write.parquet("hdfs://master:9000/dbms_project/movies.parquet")
ratingsDF = spark.read.csv("hdfs://master:9000/dbms_project/ratings.csv")
ratingsDF.write.parquet("hdfs://master:9000/dbms_project/ratings.parquet")
genresDF = spark.read.csv("hdfs://master:9000/dbms_project/movie_genres.csv")
genresDF.write.parquet("hdfs://master:9000/dbms_project/movie_genres.parquet")
