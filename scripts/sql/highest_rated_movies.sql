 SELECT movies.title, 
    COUNT(ratings.movie_id) AS rating_count,
    AVG(ratings.rating) AS avg_rating
 FROM movies 
 INNER JOIN ratings 
 ON movies.id = ratings.movie_id 
 GROUP BY movies.title 
 HAVING rating_count > 10 
 ORDER BY avg_rating DESC;