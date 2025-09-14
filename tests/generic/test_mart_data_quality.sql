-- test_mart_data_quality.sql
-- Comprehensive data quality tests for marts models

-- Test that all marts have data
with mart_counts as (
  select 
    'mart_artist_performance' as mart_name,
    count(*) as record_count
  from {{ ref('mart_artist_performance') }}
  
  union all
  
  select 
    'mart_genre_analysis' as mart_name,
    count(*) as record_count
  from {{ ref('mart_genre_analysis') }}
  
  union all
  
  select 
    'mart_review_insights' as mart_name,
    count(*) as record_count
  from {{ ref('mart_review_insights') }}
  
  union all
  
  select 
    'mart_editorial_dashboard' as mart_name,
    count(*) as record_count
  from {{ ref('mart_editorial_dashboard') }}
)

select *
from mart_counts
where record_count = 0

