SELECT primary_title as "Title",
       total_episodes as "Number of Episodes"
FROM titles
JOIN episodes ON titles.id = episodes.title_id
ORDER BY total_episodes DESC
LIMIT 50;