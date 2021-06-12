select dim_crew.name, dim_crew.age, dim_casts.name, dim_casts.age, primary_title from dim_casts
join titles_casts on cast_id = dim_casts.id
join fact_titles on cast_id = fact_titles.id
join dim_title_desc on title_desc_id = dim_title_desc.id
join titles_crew on fact_titles.id = titles_crew.title_id
join dim_crew on titles_crew.crew_id = dim_crew.id
order by num_votes desc, av_rating desc 
limit 1