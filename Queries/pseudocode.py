# --------------------- Repartition Join ----------

def map(_,v):
	k = extractJoinKey(v)
	v.addTag()
	emit(k,v)

def reduce(k,vs):
	BR , BL = [] , []
	for v in vs:
		BR.append(v) if v.tag == 'R' else BL.append(v)
	for new_record in ((l, b) for l in BL for b in BR):
		emit(k,new_record)


# --------------------- q1 -------------------------
def getEarnings(a,b):
	print("a is:" + a)
        print("b is:" + b)
        a = int(a)
        b = int(b)
        return (float(a)-float(b))/float(b) * 100

def map(_,movie):
	if(movie.release_date != None):
		if(movie.release_date.year>=2000):
			if(movie.cost >0 and movie.revenue >0):
				emit(movie.release_date.year,( movie.name,getEarnings(movie.revenue,movie.cost) ))

def reduce(k,lst):
	def maxEarnings(v):
		return v.earning
	bestEarningMovie = max(lst,maxEarnings)
	emit(k,bestEarningMovie)

def map(k,v):
	emit(1,(k,v))

def reduce(_,bestYearningMoviePerYear):
	sorted = sortByYearAscending(bestYearningMoviePerYear)
	emit(1,sorted)

# --------------------- q2 -------------------------

def map(_,rating):
	emit(rating.userID,rating.score)

def reduce(k,vs):
	emit(k , mean(vs))

def map(k,v):
	emit(1,v)

def reduce(k,vs):
	count = 0
	for v in vs:
		if v > 3:
			count+=1
	res = count / len(vs) * 100
	emit(1, res )


# --------------------- q3 -------------------------

def map(_,rating):
	emit(rating.movieID,rating.score)
def map(_,movieGenres):
	emit(movieGenres.movieID,movieGenres.genre)

# repartition join now we have (movieID,(movieRating,movieGenre))

def reduce(k,vs):
	movieMeanRating = mean(vs.rating)
	for v in vs:
		if notEmited(v.genre):
			emit(v.genre,movieMeanRating)

def map(k,v):
	emit(k,v)

def reduce(k,vs):
	meanRating = mean(vs)
	nrOfMovies = len(vs)
	emit(1,(k,meanRating,nrOfMovies))

def map(_,v):
	emit(_,v)

def reduce(_,v):
	emit(1,v)


# --------------------- q4 -------------------------

def map(_,movieGenre):
	if(movieGenre.genre == 'Drama'):
		emit(movieGenre.movieID,_)

def map(_,movie):
	if(movie.year >=2000):
		emit( movie.movieID , ( len(movie.summary) , movie.year-2000 ) )

# repartition join
# movieID,length,year
def map(k,v):
	emit( v.year % 5 , v.length)

def reduce(k,vs):
	emit(1,(k,mean(vs)))


# --------------------- q5 -------------------------
def map(_,rating):
	emit(rating.movieID,(rating.userID,rating.rating))

def map(_,movieGenre):
	emit(movieGenre.movieID,movieGenre.genre)

def map(_,movie):
	emit(movie.movieID,movie.name,movie.popularity)

#repartition join the three and get
# (movieID,(userid,rating,genre,name,popularity))

def map(k,v):
	emit( (v.userID,v.genre) , (v.name,v.rating,v.popularity) )

def reduce(k,vs):
	def customMax(a,b):
		return a if (a.rating>b.rating or (a.rating==b.rating and a.popularity>b.popularity))  else b
	mostLikedMovie = max(vs, customMax)
	def customMin(a,b):
		return a if (a.rating<b.rating or (a.rating==b.rating and a.popularity>b.popularity))  else b
	leastLikedMovie = min(vs, customMin )
	nrOfReviews = len(vs)
	emit(k,(leastLikedMovie,mostLikedMovie,nrOfReviews))

def map(k,v):
	emit(k.genre,(v,k.userID))

# genre,leastLikedMovie,mostLikedMovie,nrOfReviews,userID
def reduce(k,vs):
	userWithMostReviews = max(vs,lambda v:v.nrOfReviews)
	emit(k,userWithMostReviews)

def map(k,v):
	emit(1,(k,v))

def reduce(_,v):
	v.sortByGenreName()
	emit(1,v)