SELECT age, COUNT(age)
FROM
(
  SELECT age FROM casts where age > 0 and age < 150
  UNION ALL
  SELECT age FROM crew where age > 0 and age < 150
) as t GROUP BY age