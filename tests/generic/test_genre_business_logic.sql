-- test_genre_business_logic.sql
-- Business logic validation tests for genre analysis

-- Test that dominant genres have high review counts
select *
from {{ ref('mart_genre_analysis') }}
where market_position = 'Dominant Genre' 
  and current_count_rank > 3

union all

-- Test that high quality genres have high scores
select *
from {{ ref('mart_genre_analysis') }}
where quality_tier = 'High Quality Genre' 
  and current_score_rank > 3

union all

-- Test that high commercial value genres have good BNM rates
select *
from {{ ref('mart_genre_analysis') }}
where commercial_viability = 'High Commercial Value' 
  and current_bnm_rank > 3

union all

-- Test that emerging genres are new or have rising trends
select *
from {{ ref('mart_genre_analysis') }}
where lifecycle_stage = 'Emerging Genre' 
  and current_trend not in ('New', 'Rising')

union all

-- Test that declining genres have declining trends
select *
from {{ ref('mart_genre_analysis') }}
where lifecycle_stage = 'Declining Genre' 
  and current_trend != 'Declining'

union all

-- Test that mature genres have been active for multiple decades
select *
from {{ ref('mart_genre_analysis') }}
where lifecycle_stage = 'Mature Genre' 
  and decades_active < 2

union all

-- Test that niche genres have low review counts
select *
from {{ ref('mart_genre_analysis') }}
where market_position = 'Niche Genre' 
  and current_count_rank <= 6

union all

-- Test that investment recommendations align with trends
select *
from {{ ref('mart_genre_analysis') }}
where investment_recommendation = 'High Investment Potential' 
  and current_trend != 'Rising'

union all

-- Test that editorial priority scores are reasonable
select *
from {{ ref('mart_genre_analysis') }}
where editorial_priority_score < 1 or editorial_priority_score > 40

