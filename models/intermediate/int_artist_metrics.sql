-- int_artist_metrics.sql
-- Artist performance calculations and career trajectory analysis

with artist_reviews as (
    select
        artist_name,
        score,
        review_date,
        year,
        decade,
        best_new_music,
        reviewer,
        review_url,
        -- Calculate review number for this artist
        row_number() over (
            partition by artist_name 
            order by review_date asc
        ) as review_number,
        
        -- Calculate time since first review
        julianday(review_date) - julianday(
            min(review_date) over (partition by artist_name)
        ) as days_since_first_review
        
    from {{ ref('stg_pitchfork_reviews') }}
    where not is_artist_missing
),

artist_aggregates as (
    select
        artist_name,
        
        -- Basic metrics
        count(*) as total_reviews,
        min(review_date) as first_review_date,
        max(review_date) as latest_review_date,
        
        -- Score metrics
        avg(score) as avg_score,
        min(score) as min_score,
        max(score) as max_score,
        -- SQLite doesn't have stddev, using approximation
        sqrt(avg(score * score) - avg(score) * avg(score)) as score_stddev,
        
        -- Best new music metrics
        sum(case when best_new_music = 1 then 1 else 0 end) as best_new_music_count,
        avg(case when best_new_music = 1 then 1.0 else 0.0 end) as best_new_music_rate,
        
        -- Career span
        julianday(max(review_date)) - julianday(min(review_date)) as career_span_days,
        
        -- Reviewer diversity
        count(distinct reviewer) as unique_reviewers,
        
        -- Recent performance (last 2 years)
        avg(case 
            when julianday('now') - julianday(review_date) <= 730 
            then score 
            else null 
        end) as recent_avg_score,
        
        count(case 
            when julianday('now') - julianday(review_date) <= 730 
            then 1 
            else null 
        end) as recent_review_count
        
    from artist_reviews
    group by artist_name
),

artist_trends as (
    select
        artist_name,
        
        -- Score trend analysis
        case 
            when recent_avg_score is null then 'No recent reviews'
            when recent_avg_score > avg_score + 0.5 then 'Improving'
            when recent_avg_score < avg_score - 0.5 then 'Declining'
            else 'Stable'
        end as score_trend,
        
        -- Career stage classification
        case 
            when career_span_days <= 365 then 'New Artist'
            when career_span_days <= 1095 then 'Emerging Artist'
            when career_span_days <= 3650 then 'Established Artist'
            else 'Veteran Artist'
        end as career_stage,
        
        -- Activity level
        case 
            when total_reviews >= 10 then 'High Activity'
            when total_reviews >= 5 then 'Medium Activity'
            else 'Low Activity'
        end as activity_level,
        
        -- Quality consistency
        case 
            when score_stddev <= 1.0 then 'Consistent'
            when score_stddev <= 2.0 then 'Variable'
            else 'Inconsistent'
        end as quality_consistency
        
    from artist_aggregates
),

final as (
    select
        aa.artist_name,
        aa.total_reviews,
        aa.first_review_date,
        aa.latest_review_date,
        aa.avg_score,
        aa.min_score,
        aa.max_score,
        aa.score_stddev,
        aa.best_new_music_count,
        aa.best_new_music_rate,
        aa.career_span_days,
        aa.unique_reviewers,
        aa.recent_avg_score,
        aa.recent_review_count,
        
        -- Trend and classification metrics
        at.score_trend,
        at.career_stage,
        at.activity_level,
        at.quality_consistency,
        
        -- Calculated fields
        round(aa.avg_score, 2) as avg_score_rounded,
        round(aa.score_stddev, 2) as score_stddev_rounded,
        round(aa.best_new_music_rate * 100, 1) as best_new_music_percentage,
        
        -- Career span in years
        round(aa.career_span_days / 365.25, 1) as career_span_years,
        
        -- Data quality flags
        case when aa.total_reviews < 2 then true else false end as is_single_review_artist,
        case when aa.unique_reviewers = 1 then true else false end as is_single_reviewer_artist
        
    from artist_aggregates aa
    left join artist_trends at on aa.artist_name = at.artist_name
)

select * from final
