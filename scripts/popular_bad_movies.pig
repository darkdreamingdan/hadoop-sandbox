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
	GENERATE movieID, movieTitle;

ratingsByMovie = GROUP ratings BY movieID;

moviesWithAvgRating = FOREACH ratingsByMovie  GENERATE
   group AS movieID, AVG(ratings.rating) AS rating, COUNT(ratings.rating) AS ratingCount;

moviesWithBadRating = FILTER moviesWithAvgRating BY rating < 2.0;

moviesWithBadRatingWithNames = JOIN nameLookup BY movieID, moviesWithBadRating BY movieID;

DESCRIBE moviesWithBadRatingWithNames;

moviesWithBadRatingWithNames = ORDER moviesWithBadRatingWithNames BY ratingCount DESC;

DUMP moviesWithBadRatingWithNames;
