-- mart_artist_performance.sql
-- Artist success metrics and career trajectory analysis for business stakeholders

{{ config(materialized='table') }}

with artist_performance as (
  select
    CAST(artist_name AS TEXT) as artist_name,
    CAST(total_reviews AS INTEGER) as total_reviews,
    CAST(first_review_date AS DATE) as first_review_date,
    CAST(latest_review_date AS DATE) as latest_review_date,
    CAST(avg_score AS REAL) as avg_score,
    CAST(min_score AS REAL) as min_score,
    CAST(max_score AS REAL) as max_score,
    CAST(score_stddev AS REAL) as score_stddev,
    CAST(best_new_music_count AS INTEGER) as best_new_music_count,
    CAST(best_new_music_rate AS REAL) as best_new_music_rate,
    CAST(career_span_days AS INTEGER) as career_span_days,
    CAST(unique_reviewers AS INTEGER) as unique_reviewers,
    CAST(recent_avg_score AS REAL) as recent_avg_score,
    CAST(recent_review_count AS INTEGER) as recent_review_count,
    CAST(score_trend AS TEXT) as score_trend,
    CAST(career_stage AS TEXT) as career_stage,
    CAST(activity_level AS TEXT) as activity_level,
    CAST(quality_consistency AS TEXT) as quality_consistency,
    CAST(avg_score_rounded AS REAL) as avg_score_rounded,
    CAST(score_stddev_rounded AS REAL) as score_stddev_rounded,
    CAST(best_new_music_percentage AS REAL) as best_new_music_percentage,
    CAST(career_span_years AS REAL) as career_span_years,
    CAST(is_single_review_artist AS INTEGER) as is_single_review_artist,
    CAST(is_single_reviewer_artist AS INTEGER) as is_single_reviewer_artist
    
  from {{ ref('int_artist_metrics') }}
),

-- Calculate rankings and percentiles
artist_rankings as (
  select
    *,
    
    -- Overall rankings
    row_number() over (order by avg_score desc) as rank_by_avg_score,
    row_number() over (order by total_reviews desc) as rank_by_review_count,
    row_number() over (order by best_new_music_rate desc) as rank_by_bnm_rate,
    
    -- Percentile rankings
    ntile(10) over (order by avg_score desc) as score_percentile,
    ntile(10) over (order by total_reviews desc) as activity_percentile,
    ntile(10) over (order by best_new_music_rate desc) as quality_percentile,
    
    -- Success classification
    case 
      when avg_score >= 8.0 and total_reviews >= 5 then 'Elite Artist'
      when avg_score >= 7.5 and total_reviews >= 3 then 'High Quality Artist'
      when avg_score >= 7.0 and total_reviews >= 2 then 'Quality Artist'
      when total_reviews >= 10 then 'Prolific Artist'
      when avg_score >= 6.5 then 'Decent Artist'
      else 'Emerging Artist'
    end as success_tier,
    
    -- Career momentum
    case 
      when score_trend = 'Improving' and recent_review_count >= 2 then 'Rising Star'
      when score_trend = 'Improving' then 'On the Rise'
      when score_trend = 'Declining' and recent_review_count >= 2 then 'Declining'
      when score_trend = 'Declining' then 'Past Peak'
      when score_trend = 'Stable' and recent_review_count >= 2 then 'Consistent Performer'
      when score_trend = 'Stable' then 'Steady'
      else 'Unknown Trajectory'
    end as career_momentum,
    
    -- Commercial potential indicators
    case 
      when best_new_music_rate >= 0.3 and total_reviews >= 3 then 'High Commercial Potential'
      when best_new_music_rate >= 0.2 and total_reviews >= 2 then 'Moderate Commercial Potential'
      when avg_score >= 7.5 and total_reviews >= 3 then 'Quality Potential'
      else 'Limited Commercial Potential'
    end as commercial_potential,
    
    -- Risk assessment
    case 
      when is_single_review_artist = true then 'High Risk - Single Review'
      when is_single_reviewer_artist = true then 'Medium Risk - Single Reviewer'
      when score_stddev > 2.0 then 'Medium Risk - Inconsistent Quality'
      when recent_review_count = 0 and career_span_years > 2 then 'Medium Risk - No Recent Activity'
      else 'Low Risk'
    end as risk_level
    
  from artist_performance
),

-- Add industry benchmarks
industry_benchmarks as (
  select
    avg(avg_score) as industry_avg_score,
    avg(total_reviews) as industry_avg_reviews,
    avg(best_new_music_rate) as industry_avg_bnm_rate,
    count(*) as total_artists
  from artist_performance
),

final as (
  select
    CAST(ap.artist_name AS TEXT) as artist_name,
    
    -- Core Performance Metrics
    CAST(ap.total_reviews AS INTEGER) as total_reviews,
    CAST(ap.avg_score_rounded AS REAL) as avg_score,
    CAST(ap.best_new_music_count AS INTEGER) as best_new_music_count,
    CAST(ap.best_new_music_percentage AS REAL) as best_new_music_percentage,
    CAST(ap.career_span_years AS REAL) as career_span_years,
    
    -- Score Range
    CAST(ap.min_score AS REAL) as min_score,
    CAST(ap.max_score AS REAL) as max_score,
    CAST(ap.score_stddev_rounded AS REAL) as score_consistency,
    
    -- Career Timeline
    CAST(ap.first_review_date AS DATE) as first_review_date,
    CAST(ap.latest_review_date AS DATE) as latest_review_date,
    CAST(ap.recent_avg_score AS REAL) as recent_avg_score,
    CAST(ap.recent_review_count AS INTEGER) as recent_review_count,
    
    -- Classifications
    CAST(ap.career_stage AS TEXT) as career_stage,
    CAST(ap.activity_level AS TEXT) as activity_level,
    CAST(ap.quality_consistency AS TEXT) as quality_consistency,
    CAST(ap.score_trend AS TEXT) as score_trend,
    
    -- Rankings and Percentiles
    CAST(ar.rank_by_avg_score AS INTEGER) as rank_by_avg_score,
    CAST(ar.rank_by_review_count AS INTEGER) as rank_by_review_count,
    CAST(ar.rank_by_bnm_rate AS INTEGER) as rank_by_bnm_rate,
    CAST(ar.score_percentile AS INTEGER) as score_percentile,
    CAST(ar.activity_percentile AS INTEGER) as activity_percentile,
    CAST(ar.quality_percentile AS INTEGER) as quality_percentile,
    
    -- Business Classifications
    CAST(ar.success_tier AS TEXT) as success_tier,
    CAST(ar.career_momentum AS TEXT) as career_momentum,
    CAST(ar.commercial_potential AS TEXT) as commercial_potential,
    CAST(ar.risk_level AS TEXT) as risk_level,
    
    -- Reviewer Metrics
    CAST(ap.unique_reviewers AS INTEGER) as unique_reviewers,
    
    -- Data Quality Flags
    CAST(ap.is_single_review_artist AS INTEGER) as is_single_review_artist,
    CAST(ap.is_single_reviewer_artist AS INTEGER) as is_single_reviewer_artist,
    
    -- Industry Context
    CAST(round(ap.avg_score - ib.industry_avg_score, 2) AS REAL) as score_vs_industry,
    CAST(round(ap.total_reviews - ib.industry_avg_reviews, 1) AS REAL) as reviews_vs_industry,
    CAST(round((ap.best_new_music_rate - ib.industry_avg_bnm_rate) * 100, 1) AS REAL) as bnm_vs_industry,
    
    -- Success Score (composite metric)
    CAST(round(
      (ar.score_percentile * 0.4) + 
      (ar.activity_percentile * 0.3) + 
      (ar.quality_percentile * 0.3), 1
    ) AS REAL) as overall_success_score,
    
    -- Last Updated
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as last_updated
    
  from artist_performance ap
  cross join industry_benchmarks ib
  left join artist_rankings ar on ap.artist_name = ar.artist_name
)

select * from final
order by overall_success_score desc, avg_score desc, total_reviews desc

