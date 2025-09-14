-- mart_review_insights.sql
-- Review pattern analysis and score trends for editorial insights

{{ config(materialized='table') }}

with review_patterns as (
  select
    review_year,
    review_quarter,
    review_month,
    decade,
    total_reviews,
    unique_artists,
    unique_reviewers,
    avg_score,
    min_score,
    max_score,
    score_stddev,
    median_score,
    high_score_count,
    medium_score_count,
    low_score_count,
    high_score_percentage,
    medium_score_percentage,
    low_score_percentage,
    best_new_music_count,
    best_new_music_percentage,
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
    prev_year_avg_score,
    score_change_yoy,
    prev_year_review_count,
    review_count_change_yoy,
    score_trend_direction,
    review_volume_trend
    
  from {{ ref('int_score_trends') }}
),

-- Current year focus for recent trends
current_year_metrics as (
  select
    review_year,
    review_quarter,
    total_reviews,
    unique_artists,
    unique_reviewers,
    avg_score,
    high_score_percentage,
    medium_score_percentage,
    low_score_percentage,
    best_new_music_percentage,
    exceptional_count,
    excellent_count,
    good_count,
    score_change_yoy,
    review_count_change_yoy,
    score_trend_direction,
    review_volume_trend
    
  from review_patterns
  where review_year = (
    select max(review_year) 
    from review_patterns
  )
),

-- Historical benchmarks and trends
historical_benchmarks as (
  select
    -- Overall benchmarks
    avg(avg_score) as historical_avg_score,
    avg(total_reviews) as historical_avg_reviews_per_period,
    avg(high_score_percentage) as historical_avg_high_score_pct,
    avg(medium_score_percentage) as historical_avg_medium_score_pct,
    avg(low_score_percentage) as historical_avg_low_score_pct,
    avg(best_new_music_percentage) as historical_avg_bnm_pct,
    
    -- Score distribution benchmarks
    avg(exceptional_count) as historical_avg_exceptional,
    avg(excellent_count) as historical_avg_excellent,
    avg(good_count) as historical_avg_good,
    
    -- Trend analysis
    avg(score_change_yoy) as historical_avg_score_change,
    avg(review_count_change_yoy) as historical_avg_volume_change,
    
    -- Variability metrics
    max(avg_score) - min(avg_score) as score_range_historical,
    max(total_reviews) - min(total_reviews) as volume_range_historical
    
  from review_patterns
  where review_year < (
    select max(review_year) 
    from review_patterns
  )
),

-- Seasonal analysis
seasonal_patterns as (
  select
    review_quarter,
    review_month,
    
    -- Quarterly averages
    avg(avg_score) as quarterly_avg_score,
    avg(total_reviews) as quarterly_avg_reviews,
    avg(high_score_percentage) as quarterly_avg_high_score_pct,
    avg(best_new_music_percentage) as quarterly_avg_bnm_pct,
    
    -- Monthly patterns
    avg(case when review_month in (1,2,3) then avg_score end) as q1_avg_score,
    avg(case when review_month in (4,5,6) then avg_score end) as q2_avg_score,
    avg(case when review_month in (7,8,9) then avg_score end) as q3_avg_score,
    avg(case when review_month in (10,11,12) then avg_score end) as q4_avg_score,
    
    avg(case when review_month in (1,2,3) then total_reviews end) as q1_avg_reviews,
    avg(case when review_month in (4,5,6) then total_reviews end) as q2_avg_reviews,
    avg(case when review_month in (7,8,9) then total_reviews end) as q3_avg_reviews,
    avg(case when review_month in (10,11,12) then total_reviews end) as q4_avg_reviews
    
  from review_patterns
  group by review_quarter, review_month
),

-- Score inflation/deflation analysis
score_inflation_analysis as (
  select
    review_year,
    avg_score,
    lag(avg_score) over (order by review_year) as prev_year_score,
    
    -- Calculate cumulative score change
    avg_score - first_value(avg_score) over (order by review_year) as cumulative_score_change,
    
    -- Score inflation rate
    case 
      when lag(avg_score) over (order by review_year) > 0 then
        round((avg_score - lag(avg_score) over (order by review_year)) * 100.0 / 
              lag(avg_score) over (order by review_year), 2)
      else null
    end as score_inflation_rate
    
  from (
    select 
      review_year,
      avg(avg_score) as avg_score
    from review_patterns
    group by review_year
  )
),

-- Editorial insights and recommendations
editorial_insights as (
  select
    cym.review_year,
    cym.review_quarter,
    
    -- Current Performance
    cym.total_reviews,
    cym.unique_artists,
    cym.unique_reviewers,
    cym.avg_score,
    cym.high_score_percentage,
    cym.medium_score_percentage,
    cym.low_score_percentage,
    cym.best_new_music_percentage,
    cym.exceptional_count,
    cym.excellent_count,
    cym.good_count,
    cym.score_change_yoy,
    cym.review_count_change_yoy,
    cym.score_trend_direction,
    cym.review_volume_trend,
    
    -- Performance vs Historical Benchmarks
    round(cym.avg_score - hb.historical_avg_score, 3) as score_vs_historical,
    round(cym.total_reviews - hb.historical_avg_reviews_per_period, 1) as reviews_vs_historical,
    round(cym.high_score_percentage - hb.historical_avg_high_score_pct, 1) as high_score_vs_historical,
    round(cym.best_new_music_percentage - hb.historical_avg_bnm_pct, 1) as bnm_vs_historical,
    
    -- Editorial Alerts
    case 
      when cym.avg_score > hb.historical_avg_score + 0.3 then 'Score Inflation Alert'
      when cym.avg_score < hb.historical_avg_score - 0.3 then 'Score Deflation Alert'
      when cym.review_count_change_yoy < -20 then 'Volume Decline Alert'
      when cym.review_count_change_yoy > 50 then 'Volume Spike Alert'
      when cym.best_new_music_percentage < 5 then 'Low BNM Rate Alert'
      when cym.best_new_music_percentage > 25 then 'High BNM Rate Alert'
      else 'Normal Operations'
    end as editorial_alert,
    
    -- Quality Assessment
    case 
      when cym.exceptional_count > hb.historical_avg_exceptional * 1.5 then 'Exceptional Quality Period'
      when cym.excellent_count > hb.historical_avg_excellent * 1.2 then 'High Quality Period'
      when cym.good_count > hb.historical_avg_good * 1.1 then 'Good Quality Period'
      when cym.exceptional_count < hb.historical_avg_exceptional * 0.5 then 'Low Quality Period'
      else 'Average Quality Period'
    end as quality_assessment,
    
    -- Reviewer Activity Assessment
    round(cym.total_reviews * 1.0 / cym.unique_reviewers, 1) as avg_reviews_per_reviewer,
    round(cym.unique_artists * 100.0 / cym.total_reviews, 1) as artist_diversity_pct,
    
    -- Seasonal Context
    sp.quarterly_avg_score,
    sp.quarterly_avg_reviews,
    sp.quarterly_avg_high_score_pct,
    sp.quarterly_avg_bnm_pct,
    
    -- Score Inflation Context
    sia.score_inflation_rate,
    sia.cumulative_score_change,
    
    -- Editorial Recommendations
    case 
      when cym.score_trend_direction = 'Rising' and cym.review_volume_trend = 'Growing' then 'Increase Coverage - Growing Interest'
      when cym.score_trend_direction = 'Falling' and cym.review_volume_trend = 'Growing' then 'Maintain Coverage - Monitor Quality'
      when cym.score_trend_direction = 'Rising' and cym.review_volume_trend = 'Declining' then 'Focus on Quality - Volume Down'
      when cym.score_trend_direction = 'Falling' and cym.review_volume_trend = 'Declining' then 'Review Strategy - Both Declining'
      else 'Continue Current Strategy'
    end as editorial_recommendation,
    
    -- Content Strategy Insights
    case 
      when cym.best_new_music_percentage > 15 then 'High BNM Period - Focus on Discovery'
      when cym.exceptional_count > 5 then 'Quality Peak - Highlight Excellence'
      when round(cym.unique_artists * 100.0 / cym.total_reviews, 1) > 80 then 'High Diversity - Broad Coverage'
      when round(cym.total_reviews * 1.0 / cym.unique_reviewers, 1) > 3 then 'Reviewer Heavy Period - Balance Workload'
      else 'Standard Coverage Period'
    end as content_strategy_insight
    
  from current_year_metrics cym
  cross join historical_benchmarks hb
  left join seasonal_patterns sp on cym.review_quarter = sp.review_quarter
  left join score_inflation_analysis sia on cym.review_year = sia.review_year
),

final as (
  select
    *,
    
    -- Key Performance Indicators
    round(
      (high_score_percentage * 0.4) + 
      (best_new_music_percentage * 0.3) + 
      ((100 - abs(score_vs_historical * 10)) * 0.3), 1
    ) as editorial_kpi_score,
    
    -- Trend Strength
    case 
      when abs(score_change_yoy) > 0.2 then 'Strong Trend'
      when abs(score_change_yoy) > 0.1 then 'Moderate Trend'
      when abs(score_change_yoy) > 0.05 then 'Weak Trend'
      else 'Stable'
    end as trend_strength,
    
    -- Last Updated
    current_timestamp as last_updated
    
  from editorial_insights
)

select * from final
order by review_year desc, review_quarter desc

