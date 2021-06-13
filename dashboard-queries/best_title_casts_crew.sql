select crew.name as crew_name, crew.age as crew_age, casts.name as cast_name, casts.age as cast_age, primary_title from casts
join titles_casts on cast_id = casts.id
join titles on title_id = titles.id
join titles_crew on titles.id = titles_crew.title_id
join crew on titles_crew.crew_id = crew.id
order by num_votes desc, av_rating desc 
limit 10