SELECT age, COUNT(age)
FROM
(
  SELECT age FROM dim_casts where age > 0 and age < 150
  UNION ALL
  SELECT age FROM dim_crew where age > 0 and age < 150
) as t GROUP BY age