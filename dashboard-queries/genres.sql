SELECT genres, COUNT(genres)
FROM
(
  SELECT genre_1 as genres FROM dim_title_desc
  UNION ALL
  SELECT genre_2 as genres FROM dim_title_desc
  UNION ALL
  SELECT genre_3 as genres FROM dim_title_desc  
) as t 
WHERE genres != '\N' 
GROUP BY genres
ORDER BY count