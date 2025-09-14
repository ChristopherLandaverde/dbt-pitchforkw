-- int_reviewer_metrics.sql
-- Reviewer performance and analysis patterns

with reviewer_analysis as (
    select
        reviewer,
        score,
        review_date,
        year,
        decade,
        artist_name,
        album_name,
        best_new_music,
        review_url,
        
        -- Calculate review number for this reviewer
        row_number() over (
            partition by reviewer 
            order by review_date asc
        ) as review_number,
        
        -- Calculate days since first review
        julianday(review_date) - julianday(
            min(review_date) over (partition by reviewer)
        ) as days_since_first_review
        
    from {{ ref('stg_pitchfork_reviews') }}
    where reviewer is not null 
      and trim(reviewer) != ''
),

reviewer_aggregates as (
    select
        reviewer,
        
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
        -- SQLite doesn't have median, using approximation
        (min(score) + max(score)) / 2.0 as median_score,
        
        -- Best new music metrics
        sum(case when best_new_music = 1 then 1 else 0 end) as best_new_music_count,
        avg(case when best_new_music = 1 then 1.0 else 0.0 end) as best_new_music_rate,
        
        -- Career span
        julianday(max(review_date)) - julianday(min(review_date)) as career_span_days,
        
        -- Artist diversity
        count(distinct artist_name) as unique_artists_reviewed,
        
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
        
    from reviewer_analysis
    group by reviewer
),

reviewer_trends as (
    select
        reviewer,
        
        -- Score trend analysis
        case 
            when recent_avg_score is null then 'No recent reviews'
            when recent_avg_score > avg_score + 0.3 then 'Becoming more generous'
            when recent_avg_score < avg_score - 0.3 then 'Becoming more critical'
            else 'Stable scoring'
        end as score_trend,
        
        -- Reviewer experience level
        case 
            when total_reviews >= 100 then 'Veteran'
            when total_reviews >= 50 then 'Experienced'
            when total_reviews >= 20 then 'Mid-level'
            when total_reviews >= 5 then 'Junior'
            else 'New'
        end as experience_level,
        
        -- Activity level
        case 
            when total_reviews >= 200 then 'Very Active'
            when total_reviews >= 100 then 'Active'
            when total_reviews >= 50 then 'Moderate'
            else 'Low Activity'
        end as activity_level,
        
        -- Scoring consistency
        case 
            when score_stddev <= 0.8 then 'Very Consistent'
            when score_stddev <= 1.2 then 'Consistent'
            when score_stddev <= 1.8 then 'Variable'
            else 'Inconsistent'
        end as scoring_consistency,
        
        -- Generosity level (compared to overall average)
        case 
            when avg_score >= 7.5 then 'Very Generous'
            when avg_score >= 7.0 then 'Generous'
            when avg_score >= 6.5 then 'Moderate'
            when avg_score >= 6.0 then 'Critical'
            else 'Very Critical'
        end as generosity_level
        
    from reviewer_aggregates
),

reviewer_specializations as (
    select
        reviewer,
        
        -- Genre preferences (based on artist name patterns)
        sum(case 
            when lower(artist_name) like '%rock%' or 
                 lower(artist_name) like '%metal%' or
                 lower(artist_name) like '%punk%' then 1 
            else 0 
        end) as rock_reviews,
        
        sum(case 
            when lower(artist_name) like '%pop%' or
                 lower(artist_name) like '%dance%' or
                 lower(artist_name) like '%electronic%' then 1 
            else 0 
        end) as pop_electronic_reviews,
        
        sum(case 
            when lower(artist_name) like '%jazz%' or
                 lower(artist_name) like '%blues%' or
                 lower(artist_name) like '%soul%' then 1 
            else 0 
        end) as jazz_blues_reviews,
        
        sum(case 
            when lower(artist_name) like '%hip%' or
                 lower(artist_name) like '%rap%' or
                 lower(artist_name) like '%r&b%' then 1 
            else 0 
        end) as hip_hop_rb_reviews,
        
        sum(case 
            when lower(artist_name) like '%folk%' or
                 lower(artist_name) like '%country%' or
                 lower(artist_name) like '%americana%' then 1 
            else 0 
        end) as folk_country_reviews,
        
        sum(case 
            when lower(artist_name) like '%experimental%' or
                 lower(artist_name) like '%avant%' or
                 lower(artist_name) like '%noise%' then 1 
            else 0 
        end) as experimental_reviews
        
    from reviewer_analysis
    group by reviewer
),

final as (
    select
        ra.reviewer,
        ra.total_reviews,
        ra.first_review_date,
        ra.latest_review_date,
        round(ra.avg_score, 3) as avg_score,
        ra.min_score,
        ra.max_score,
        round(ra.score_stddev, 3) as score_stddev,
        round(ra.median_score, 3) as median_score,
        ra.best_new_music_count,
        round(ra.best_new_music_rate * 100, 1) as best_new_music_percentage,
        ra.career_span_days,
        ra.unique_artists_reviewed,
        ra.recent_avg_score,
        ra.recent_review_count,
        
        -- Trend and classification metrics
        rt.score_trend,
        rt.experience_level,
        rt.activity_level,
        rt.scoring_consistency,
        rt.generosity_level,
        
        -- Specialization metrics
        rs.rock_reviews,
        rs.pop_electronic_reviews,
        rs.jazz_blues_reviews,
        rs.hip_hop_rb_reviews,
        rs.folk_country_reviews,
        rs.experimental_reviews,
        
        -- Calculated fields
        round(ra.career_span_days / 365.25, 1) as career_span_years,
        round(ra.unique_artists_reviewed * 100.0 / ra.total_reviews, 1) as artist_diversity_percentage,
        
        -- Data quality flags
        case when ra.total_reviews < 3 then true else false end as is_occasional_reviewer,
        case when ra.unique_artists_reviewed = 1 then true else false end as is_single_artist_reviewer
        
    from reviewer_aggregates ra
    left join reviewer_trends rt on ra.reviewer = rt.reviewer
    left join reviewer_specializations rs on ra.reviewer = rs.reviewer
)

select * from final
order by total_reviews desc
