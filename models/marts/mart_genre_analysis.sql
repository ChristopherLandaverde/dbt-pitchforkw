-- mart_genre_analysis.sql
-- Genre trend analysis and health metrics for editorial decision-making

{{ config(materialized='table') }}

with genre_performance as (
  select
    primary_genre,
    review_decade,
    decade_review_count,
    decade_unique_artists,
    decade_avg_score,
    decade_best_new_music_percentage,
    prev_decade_review_count,
    decade_growth_rate,
    decade_rank_by_count,
    decade_rank_by_score,
    decade_rank_by_bnm_rate,
    trend_direction,
    genre_health_score
    
  from {{ ref('int_genre_trends') }}
),

-- Current decade focus (most recent data)
current_genre_metrics as (
  select
    primary_genre,
    decade_review_count as current_review_count,
    decade_unique_artists as current_artist_count,
    decade_avg_score as current_avg_score,
    decade_best_new_music_percentage as current_bnm_percentage,
    decade_rank_by_count as current_count_rank,
    decade_rank_by_score as current_score_rank,
    decade_rank_by_bnm_rate as current_bnm_rank,
    trend_direction as current_trend,
    genre_health_score as current_health_score
    
  from genre_performance
  where review_decade = (
    select max(review_decade) 
    from genre_performance
  )
),

-- Historical performance across decades
genre_history as (
  select
    primary_genre,
    
    -- Multi-decade metrics
    count(distinct review_decade) as decades_active,
    sum(decade_review_count) as total_all_time_reviews,
    sum(decade_unique_artists) as total_all_time_artists,
    avg(decade_avg_score) as all_time_avg_score,
    avg(decade_best_new_music_percentage) as all_time_bnm_percentage,
    
    -- Best decade performance
    max(decade_review_count) as peak_decade_reviews,
    max(decade_avg_score) as peak_decade_score,
    
    -- Consistency metrics
    min(decade_avg_score) as worst_decade_score,
    max(decade_avg_score) - min(decade_avg_score) as score_volatility,
    
    -- Growth patterns
    max(decade_growth_rate) as max_growth_rate,
    min(decade_growth_rate) as min_growth_rate,
    avg(decade_growth_rate) as avg_growth_rate
    
  from genre_performance
  group by primary_genre
),

-- Genre lifecycle analysis
genre_lifecycle as (
  select
    primary_genre,
    
    -- Lifecycle stage classification
    case 
      when decades_active = 1 and current_trend = 'New' then 'Emerging Genre'
      when decades_active = 1 and current_trend in ('Rising', 'Stable') then 'New Genre'
      when current_trend = 'Rising' and avg_growth_rate > 10 then 'Growing Genre'
      when current_trend = 'Declining' and avg_growth_rate < -10 then 'Declining Genre'
      when current_trend = 'Stable' and decades_active >= 2 then 'Mature Genre'
      when decades_active >= 2 then 'Established Genre'
      else 'Uncertain Stage'
    end as lifecycle_stage,
    
    -- Market position
    case 
      when current_count_rank <= 3 then 'Dominant Genre'
      when current_count_rank <= 6 then 'Major Genre'
      when current_count_rank <= 10 then 'Significant Genre'
      else 'Niche Genre'
    end as market_position,
    
    -- Quality tier
    case 
      when current_score_rank <= 3 then 'High Quality Genre'
      when current_score_rank <= 6 then 'Quality Genre'
      when current_score_rank <= 10 then 'Average Quality Genre'
      else 'Lower Quality Genre'
    end as quality_tier,
    
    -- Commercial viability
    case 
      when current_bnm_rank <= 3 and current_review_count >= 50 then 'High Commercial Value'
      when current_bnm_rank <= 6 and current_review_count >= 20 then 'Moderate Commercial Value'
      when current_review_count >= 10 then 'Limited Commercial Value'
      else 'Minimal Commercial Value'
    end as commercial_viability,
    
    -- Investment recommendation
    case 
      when current_trend = 'Rising' and current_health_score <= 5 then 'High Investment Potential'
      when current_trend = 'Rising' and current_health_score <= 8 then 'Moderate Investment Potential'
      when current_trend = 'Stable' and current_health_score <= 5 then 'Stable Investment'
      when current_trend = 'Declining' then 'Limited Investment'
      else 'Monitor Closely'
    end as investment_recommendation
    
  from current_genre_metrics cgm
  left join genre_history gh on cgm.primary_genre = gh.primary_genre
),

-- Genre comparisons and benchmarks
genre_benchmarks as (
  select
    avg(current_review_count) as avg_review_count,
    avg(current_avg_score) as avg_score_across_genres,
    avg(current_bnm_percentage) as avg_bnm_percentage,
    avg(current_health_score) as avg_health_score
  from current_genre_metrics
)

select
  cgm.primary_genre,
  
  -- Current Performance (Most Recent Decade)
  cgm.current_review_count,
  cgm.current_artist_count,
  cgm.current_avg_score,
  cgm.current_bnm_percentage,
  cgm.current_count_rank,
  cgm.current_score_rank,
  cgm.current_bnm_rank,
  cgm.current_trend,
  cgm.current_health_score,
  
  -- Historical Context
  gh.decades_active,
  gh.total_all_time_reviews,
  gh.total_all_time_artists,
  round(gh.all_time_avg_score, 2) as all_time_avg_score,
  round(gh.all_time_bnm_percentage, 1) as all_time_bnm_percentage,
  gh.peak_decade_reviews,
  round(gh.peak_decade_score, 2) as peak_decade_score,
  round(gh.score_volatility, 2) as score_volatility,
  round(gh.avg_growth_rate, 1) as avg_growth_rate,
  
  -- Classifications
  gl.lifecycle_stage,
  gl.market_position,
  gl.quality_tier,
  gl.commercial_viability,
  gl.investment_recommendation,
  
  -- Performance vs Benchmarks
  round(cgm.current_review_count - gb.avg_review_count, 1) as reviews_vs_average,
  round(cgm.current_avg_score - gb.avg_score_across_genres, 2) as score_vs_average,
  round(cgm.current_bnm_percentage - gb.avg_bnm_percentage, 1) as bnm_vs_average,
  round(cgm.current_health_score - gb.avg_health_score, 2) as health_vs_average,
  
  -- Key Metrics for Editorial Decision-Making
  round(cgm.current_artist_count * 100.0 / cgm.current_review_count, 1) as avg_reviews_per_artist,
  round(cgm.current_bnm_percentage * cgm.current_review_count / 100.0, 0) as estimated_bnm_count,
  
  -- Trend Strength
  case 
    when abs(gh.avg_growth_rate) > 20 then 'Strong Trend'
    when abs(gh.avg_growth_rate) > 10 then 'Moderate Trend'
    when abs(gh.avg_growth_rate) > 5 then 'Weak Trend'
    else 'Stable'
  end as trend_strength,
  
  -- Editorial Priority Score (composite metric for editorial decisions)
  round(
    (11 - cgm.current_count_rank) * 0.3 +  -- Market share weight
    (11 - cgm.current_score_rank) * 0.25 + -- Quality weight
    (11 - cgm.current_bnm_rank) * 0.25 +   -- Commercial weight
    (11 - cgm.current_health_score) * 0.2  -- Health weight
  , 1) as editorial_priority_score,
  
  -- Last Updated
  current_timestamp as last_updated
  
from current_genre_metrics cgm
left join genre_history gh on cgm.primary_genre = gh.primary_genre
left join genre_lifecycle gl on cgm.primary_genre = gl.primary_genre
cross join genre_benchmarks gb
order by editorial_priority_score desc, current_review_count desc

