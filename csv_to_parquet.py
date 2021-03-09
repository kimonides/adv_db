#!/usr/bin/env python3.7
import pandas as pd

df_1 = pd.read_csv('/home/user/project/movies.csv')
df_1.to_parquet('/home/user/project/movies.parquet')

df_2 = pd.read_csv('/home/user/project/movie_genres.csv')
df_2.to_parquet('/home/user/project/movie_genres.parquet')

df_3 = pd.read_csv('/home/user/project/ratings.csv')
df_3.to_parquet('/home/user/project/ratings.parquet')

