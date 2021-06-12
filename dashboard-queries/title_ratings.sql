SELECT primary_title as "Title",
       num_votes as "Number of Votes",
       av_rating as "Average Rating"
FROM dim_title_desc
ORDER BY num_votes DESC,
         av_rating DESC
LIMIT 50