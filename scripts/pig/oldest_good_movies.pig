ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (
	userID:int, 
    movieID:int, 
    rating:int, 
    ratingTime:int
);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|') AS (
	movieID:int,
	movieTitle:chararray,
    releaseDate:chararray,
    videoRelease:chararray,
    imdbLink:chararray
);    

nameLookup = FOREACH metadata 
	GENERATE movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) 
    AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;

DESCRIBE ratingsByMovie;
a = LIMIT ratingsByMovie 5;
DUMP a;

moviesWithAvgRating = FOREACH ratingsByMovie  GENERATE
   group AS movieID, AVG(ratings.rating) AS rating;

DESCRIBE moviesWithAvgRating;

moviesWithGoodRating = FILTER moviesWithAvgRating BY rating > 4.0;

DESCRIBE moviesWithGoodRating;

moviesWithGoodRatingWithNames = JOIN nameLookup BY movieID, moviesWithGoodRating BY movieID;

DESCRIBE moviesWithGoodRatingWithNames;

moviesWithGoodRatingWithNames = ORDER moviesWithGoodRatingWithNames BY releaseTime ASC;

DUMP moviesWithGoodRatingWithNames;