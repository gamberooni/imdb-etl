SELECT genres, COUNT(genres)
FROM
(
  SELECT genre_1 as genres FROM titles
  UNION ALL
  SELECT genre_2 as genres FROM titles
  UNION ALL
  SELECT genre_3 as genres FROM titles  
) as t 
WHERE genres != '\N' 
GROUP BY genres
ORDER BY count