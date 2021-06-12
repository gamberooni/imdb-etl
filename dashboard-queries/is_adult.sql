SELECT is_adult,
       COUNT(is_adult) as "Adult Title Count"
FROM dim_title_desc
GROUP BY is_adult