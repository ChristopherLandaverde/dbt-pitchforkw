-- int_genre_trends.sql
-- Genre analysis and trending patterns over time

with genre_classification as (
    select
        *,
        -- Basic genre classification based on artist name patterns
        case 
            when lower(artist_name) like '%rock%' or 
                 lower(artist_name) like '%metal%' or
                 lower(artist_name) like '%punk%' then 'Rock'
            when lower(artist_name) like '%pop%' or
                 lower(artist_name) like '%dance%' or
                 lower(artist_name) like '%electronic%' then 'Pop/Electronic'
            when lower(artist_name) like '%jazz%' or
                 lower(artist_name) like '%blues%' or
                 lower(artist_name) like '%soul%' then 'Jazz/Blues'
            when lower(artist_name) like '%hip%' or
                 lower(artist_name) like '%rap%' or
                 lower(artist_name) like '%r&b%' then 'Hip-Hop/R&B'
            when lower(artist_name) like '%folk%' or
                 lower(artist_name) like '%country%' or
                 lower(artist_name) like '%americana%' then 'Folk/Country'
            when lower(artist_name) like '%classical%' or
                 lower(artist_name) like '%orchestral%' then 'Classical'
            when lower(artist_name) like '%experimental%' or
                 lower(artist_name) like '%avant%' or
                 lower(artist_name) like '%noise%' then 'Experimental'
            else 'Other'
        end as primary_genre,
        
        -- Decade-based genre trends
        case 
            when decade = '2000s' then '2000s'
            when decade = '2010s' then '2010s'
            when decade = '2020s' then '2020s'
            else 'Unknown'
        end as review_decade
        
    from {{ ref('stg_pitchfork_reviews') }}
    where not is_artist_missing
),

genre_metrics as (
    select
        primary_genre,
        review_decade,
        year,
        
        -- Basic counts
        count(*) as review_count,
        count(distinct artist_name) as unique_artists,
        count(distinct reviewer) as unique_reviewers,
        
        -- Score metrics
        avg(score) as avg_score,
        min(score) as min_score,
        max(score) as max_score,
        -- SQLite doesn't have stddev, using approximation
        sqrt(avg(score * score) - avg(score) * avg(score)) as score_stddev,
        
        -- Best new music metrics
        sum(case when best_new_music = 1 then 1 else 0 end) as best_new_music_count,
        avg(case when best_new_music = 1 then 1.0 else 0.0 end) as best_new_music_rate,
        
        -- Temporal metrics
        min(review_date) as first_review_date,
        max(review_date) as latest_review_date
        
    from genre_classification
    group by primary_genre, review_decade, year
),

genre_trends as (
    select
        primary_genre,
        review_decade,
        
        -- Decade-level metrics
        sum(review_count) as decade_review_count,
        sum(unique_artists) as decade_unique_artists,
        avg(avg_score) as decade_avg_score,
        avg(best_new_music_rate) as decade_best_new_music_rate,
        
        -- Year-over-year growth
        lag(sum(review_count)) over (
            partition by primary_genre 
            order by review_decade
        ) as prev_decade_review_count,
        
        -- Calculate growth rate
        case 
            when lag(sum(review_count)) over (
                partition by primary_genre 
                order by review_decade
            ) > 0 then
                round(
                    (sum(review_count) - lag(sum(review_count)) over (
                        partition by primary_genre 
                        order by review_decade
                    )) * 100.0 / lag(sum(review_count)) over (
                        partition by primary_genre 
                        order by review_decade
                    ), 2
                )
            else null
        end as decade_growth_rate
        
    from genre_metrics
    group by primary_genre, review_decade
),

genre_rankings as (
    select
        *,
        -- Rank genres by review count within each decade
        row_number() over (
            partition by review_decade 
            order by decade_review_count desc
        ) as decade_rank_by_count,
        
        -- Rank genres by average score within each decade
        row_number() over (
            partition by review_decade 
            order by decade_avg_score desc
        ) as decade_rank_by_score,
        
        -- Rank genres by best new music rate within each decade
        row_number() over (
            partition by review_decade 
            order by decade_best_new_music_rate desc
        ) as decade_rank_by_bnm_rate
        
    from genre_trends
),

final as (
    select
        primary_genre,
        review_decade,
        decade_review_count,
        decade_unique_artists,
        round(decade_avg_score, 2) as decade_avg_score,
        round(decade_best_new_music_rate * 100, 1) as decade_best_new_music_percentage,
        prev_decade_review_count,
        decade_growth_rate,
        
        -- Rankings
        decade_rank_by_count,
        decade_rank_by_score,
        decade_rank_by_bnm_rate,
        
        -- Trend indicators
        case 
            when decade_growth_rate > 20 then 'Rising'
            when decade_growth_rate < -20 then 'Declining'
            when decade_growth_rate is null then 'New'
            else 'Stable'
        end as trend_direction,
        
        -- Genre health score (composite metric)
        round(
            (decade_rank_by_count * 0.4 + 
             decade_rank_by_score * 0.3 + 
             decade_rank_by_bnm_rate * 0.3), 2
        ) as genre_health_score
        
    from genre_rankings
)

select * from final
order by review_decade desc, decade_review_count desc
