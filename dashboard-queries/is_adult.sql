SELECT is_adult,
       COUNT(is_adult) as "Adult Title Count"
FROM titles
GROUP BY is_adult

-- grafana version
SELECT 
(SELECT COUNT(is_adult) FROM titles WHERE is_adult = True) as "Adult",
(SELECT COUNT(is_adult) FROM titles WHERE is_adult = False) as "Non-adult"