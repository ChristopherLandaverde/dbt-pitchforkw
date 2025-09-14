-- Simple test model to verify dbt is working
select 
    reviewid as review_id,
    artist,
    title as album_name,
    score,
    pub_date as review_date
from {{ source('raw_pitchfork', 'reviews') }}
limit 10
