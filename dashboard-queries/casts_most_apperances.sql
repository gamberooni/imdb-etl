SELECT name, count(name) as c FROM casts 
join titles_casts on cast_id = id
group by name order by c desc limit 100;