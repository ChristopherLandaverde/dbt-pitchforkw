-- test_review_insights_business_logic.sql
-- Business logic validation tests for review insights

-- Test that rising trends have positive score changes
select *
from {{ ref('mart_review_insights') }}
where score_trend_direction = 'Rising' 
  and score_change_yoy <= 0

union all

-- Test that falling trends have negative score changes
select *
from {{ ref('mart_review_insights') }}
where score_trend_direction = 'Falling' 
  and score_change_yoy >= 0

union all

-- Test that growing volume trends have positive review count changes
select *
from {{ ref('mart_review_insights') }}
where review_volume_trend = 'Growing' 
  and review_count_change_yoy <= 0

union all

-- Test that declining volume trends have negative review count changes
select *
from {{ ref('mart_review_insights') }}
where review_volume_trend = 'Declining' 
  and review_count_change_yoy >= 0

union all

-- Test that score percentages sum to approximately 100%
select *
from {{ ref('mart_review_insights') }}
where abs((high_score_percentage + medium_score_percentage + low_score_percentage) - 100) > 5

union all

-- Test that review counts are consistent with unique counts
select *
from {{ ref('mart_review_insights') }}
where total_reviews < unique_artists or total_reviews < unique_reviewers

union all

-- Test that exceptional counts are reasonable relative to total reviews
select *
from {{ ref('mart_review_insights') }}
where exceptional_count > total_reviews * 0.1  -- More than 10% exceptional seems high

union all

-- Test that editorial alerts are triggered appropriately
select *
from {{ ref('mart_review_insights') }}
where editorial_alert like '%Alert%' 
  and not (
    (editorial_alert = 'Score Inflation Alert' and score_vs_historical > 0.3) or
    (editorial_alert = 'Score Deflation Alert' and score_vs_historical < -0.3) or
    (editorial_alert = 'Volume Decline Alert' and review_count_change_yoy < -20) or
    (editorial_alert = 'Volume Spike Alert' and review_count_change_yoy > 50) or
    (editorial_alert = 'Low BNM Rate Alert' and best_new_music_percentage < 5) or
    (editorial_alert = 'High BNM Rate Alert' and best_new_music_percentage > 25)
  )

union all

-- Test that quality assessments align with score distributions
select *
from {{ ref('mart_review_insights') }}
where quality_assessment = 'Exceptional Quality Period' 
  and exceptional_count <= 2

union all

-- Test that KPI scores are reasonable
select *
from {{ ref('mart_review_insights') }}
where editorial_kpi_score < 0 or editorial_kpi_score > 100

