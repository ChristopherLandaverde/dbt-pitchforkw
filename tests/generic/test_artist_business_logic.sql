-- test_artist_business_logic.sql
-- Business logic validation tests for artist performance metrics

-- Test that artists with high activity have reasonable review counts
select *
from {{ ref('mart_artist_performance') }}
where activity_level = 'High Activity' 
  and total_reviews < 5

union all

-- Test that elite artists have high scores
select *
from {{ ref('mart_artist_performance') }}
where success_tier = 'Elite Artist' 
  and avg_score < 8.0

union all

-- Test that veteran artists have long career spans
select *
from {{ ref('mart_artist_performance') }}
where career_stage = 'Veteran Artist' 
  and career_span_years < 10

union all

-- Test that improving artists have positive score trends
select *
from {{ ref('mart_artist_performance') }}
where score_trend = 'Improving' 
  and recent_avg_score <= avg_score

union all

-- Test that declining artists have negative score trends
select *
from {{ ref('mart_artist_performance') }}
where score_trend = 'Declining' 
  and recent_avg_score >= avg_score

union all

-- Test that commercial potential aligns with BNM rate
select *
from {{ ref('mart_artist_performance') }}
where commercial_potential = 'High Commercial Potential' 
  and best_new_music_percentage < 20

union all

-- Test that risk levels are correctly assigned
select *
from {{ ref('mart_artist_performance') }}
where risk_level = 'High Risk - Single Review' 
  and not is_single_review_artist

union all

select *
from {{ ref('mart_artist_performance') }}
where risk_level = 'Medium Risk - Single Reviewer' 
  and not is_single_reviewer_artist

