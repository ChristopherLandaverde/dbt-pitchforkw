-- stg_pitchfork_reviews_v2.sql
-- Clean and standardize raw Pitchfork review data

with source_data as (
    select * from {{ source('raw_pitchfork', 'reviews') }}
),

cleaned_data as (
    select
        -- Primary key
        reviewid as review_id,
        
        -- Artist information
        trim(artist) as artist_name,
        
        -- Album information
        trim(title) as album_name,
        
        -- Review details
        score,
        pub_date as review_date,
        trim(author) as reviewer,
        best_new_music,
        url as review_url,
        
        -- Date components for analysis (using existing year/month columns)
        pub_year as year,
        pub_month as month,
        case 
            when pub_year < 2010 then '2000s'
            when pub_year < 2020 then '2010s'
            else '2020s'
        end as decade,
        
        -- Data quality flags
        case 
            when score is null then true 
            else false 
        end as is_score_missing,
        
        case 
            when trim(artist) = '' or artist is null then true 
            else false 
        end as is_artist_missing,
        
        case 
            when trim(title) = '' or title is null then true 
            else false 
        end as is_album_missing
        
    from source_data
),

final as (
    select
        *,
        -- Add row number for deduplication if needed
        row_number() over (
            partition by review_id 
            order by review_date desc
        ) as row_num
        
    from cleaned_data
)

select
    review_id,
    artist_name,
    album_name,
    score,
    review_date,
    reviewer,
    best_new_music,
    review_url,
    year,
    month,
    decade,
    is_score_missing,
    is_artist_missing,
    is_album_missing
    
from final
where row_num = 1  -- Remove any duplicates
