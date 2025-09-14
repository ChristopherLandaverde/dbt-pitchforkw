-- int_score_trends.sql
-- Review score analysis and trending patterns

with score_analysis as (
    select
        *,
        -- Score categorization
        case 
            when score >= 9.0 then 'Exceptional (9.0+)'
            when score >= 8.0 then 'Excellent (8.0-8.9)'
            when score >= 7.0 then 'Good (7.0-7.9)'
            when score >= 6.0 then 'Decent (6.0-6.9)'
            when score >= 5.0 then 'Average (5.0-5.9)'
            when score >= 4.0 then 'Below Average (4.0-4.9)'
            when score >= 3.0 then 'Poor (3.0-3.9)'
            when score >= 2.0 then 'Very Poor (2.0-2.9)'
            when score >= 1.0 then 'Terrible (1.0-1.9)'
            else 'Awful (0.0-0.9)'
        end as score_category,
        
        -- Score tier
        case 
            when score >= 8.0 then 'High'
            when score >= 6.0 then 'Medium'
            else 'Low'
        end as score_tier,
        
        -- Year and month for temporal analysis
        cast(substr(review_date, 1, 4) as integer) as review_year,
        cast(substr(review_date, 6, 2) as integer) as review_month,
        
        -- Quarter calculation
        case 
            when cast(substr(review_date, 6, 2) as integer) in (1, 2, 3) then 'Q1'
            when cast(substr(review_date, 6, 2) as integer) in (4, 5, 6) then 'Q2'
            when cast(substr(review_date, 6, 2) as integer) in (7, 8, 9) then 'Q3'
            else 'Q4'
        end as review_quarter
        
    from {{ ref('stg_pitchfork_reviews') }}
    where not is_score_missing
),

temporal_metrics as (
    select
        review_year,
        review_quarter,
        review_month,
        decade,
        
        -- Basic counts
        count(*) as total_reviews,
        count(distinct artist_name) as unique_artists,
        count(distinct reviewer) as unique_reviewers,
        
        -- Score statistics
        avg(score) as avg_score,
        min(score) as min_score,
        max(score) as max_score,
        -- SQLite doesn't have stddev, using approximation
        sqrt(avg(score * score) - avg(score) * avg(score)) as score_stddev,
        -- SQLite doesn't have median, using approximation
        (min(score) + max(score)) / 2.0 as median_score,
        
        -- Score distribution
        sum(case when score >= 8.0 then 1 else 0 end) as high_score_count,
        sum(case when score >= 6.0 and score < 8.0 then 1 else 0 end) as medium_score_count,
        sum(case when score < 6.0 then 1 else 0 end) as low_score_count,
        
        -- Best new music metrics
        sum(case when best_new_music = 1 then 1 else 0 end) as best_new_music_count,
        avg(case when best_new_music = 1 then 1.0 else 0.0 end) as best_new_music_rate,
        
        -- Score category distribution
        sum(case when score_category = 'Exceptional (9.0+)' then 1 else 0 end) as exceptional_count,
        sum(case when score_category = 'Excellent (8.0-8.9)' then 1 else 0 end) as excellent_count,
        sum(case when score_category = 'Good (7.0-7.9)' then 1 else 0 end) as good_count,
        sum(case when score_category = 'Decent (6.0-6.9)' then 1 else 0 end) as decent_count,
        sum(case when score_category = 'Average (5.0-5.9)' then 1 else 0 end) as average_count,
        sum(case when score_category = 'Below Average (4.0-4.9)' then 1 else 0 end) as below_average_count,
        sum(case when score_category = 'Poor (3.0-3.9)' then 1 else 0 end) as poor_count,
        sum(case when score_category = 'Very Poor (2.0-2.9)' then 1 else 0 end) as very_poor_count,
        sum(case when score_category = 'Terrible (1.0-1.9)' then 1 else 0 end) as terrible_count,
        sum(case when score_category = 'Awful (0.0-0.9)' then 1 else 0 end) as awful_count
        
    from score_analysis
    group by review_year, review_quarter, review_month, decade
),

score_trends as (
    select
        *,
        -- Year-over-year score trends
        lag(avg_score) over (
            partition by review_quarter 
            order by review_year
        ) as prev_year_avg_score,
        
        lag(total_reviews) over (
            partition by review_quarter 
            order by review_year
        ) as prev_year_review_count,
        
        -- Calculate year-over-year changes
        case 
            when lag(avg_score) over (
                partition by review_quarter 
                order by review_year
            ) is not null then
                round(
                    avg_score - lag(avg_score) over (
                        partition by review_quarter 
                        order by review_year
                    ), 3
                )
            else null
        end as score_change_yoy,
        
        case 
            when lag(total_reviews) over (
                partition by review_quarter 
                order by review_year
            ) > 0 then
                round(
                    (total_reviews - lag(total_reviews) over (
                        partition by review_quarter 
                        order by review_year
                    )) * 100.0 / lag(total_reviews) over (
                        partition by review_quarter 
                        order by review_year
                    ), 2
                )
            else null
        end as review_count_change_yoy
        
    from temporal_metrics
),

final as (
    select
        review_year,
        review_quarter,
        review_month,
        decade,
        
        -- Basic metrics
        total_reviews,
        unique_artists,
        unique_reviewers,
        
        -- Score metrics
        round(avg_score, 3) as avg_score,
        min_score,
        max_score,
        round(score_stddev, 3) as score_stddev,
        round(median_score, 3) as median_score,
        
        -- Score distribution
        high_score_count,
        medium_score_count,
        low_score_count,
        round(high_score_count * 100.0 / total_reviews, 1) as high_score_percentage,
        round(medium_score_count * 100.0 / total_reviews, 1) as medium_score_percentage,
        round(low_score_count * 100.0 / total_reviews, 1) as low_score_percentage,
        
        -- Best new music
        best_new_music_count,
        round(best_new_music_rate * 100, 1) as best_new_music_percentage,
        
        -- Score category distribution
        exceptional_count,
        excellent_count,
        good_count,
        decent_count,
        average_count,
        below_average_count,
        poor_count,
        very_poor_count,
        terrible_count,
        awful_count,
        
        -- Trend analysis
        prev_year_avg_score,
        score_change_yoy,
        prev_year_review_count,
        review_count_change_yoy,
        
        -- Trend indicators
        case 
            when score_change_yoy > 0.1 then 'Rising'
            when score_change_yoy < -0.1 then 'Falling'
            else 'Stable'
        end as score_trend_direction,
        
        case 
            when review_count_change_yoy > 10 then 'Growing'
            when review_count_change_yoy < -10 then 'Declining'
            else 'Stable'
        end as review_volume_trend
        
    from score_trends
)

select * from final
order by review_year desc, review_quarter desc, review_month desc
