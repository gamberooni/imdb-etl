SELECT primary_title as "Title",
       total_episodes as "Number of Episodes"
FROM dim_title_desc
JOIN dim_episodes ON dim_title_desc.tconst = dim_episodes.tconst
ORDER BY total_episodes DESC
LIMIT 50;