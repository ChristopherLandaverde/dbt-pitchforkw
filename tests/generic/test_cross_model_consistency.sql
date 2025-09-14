-- test_cross_model_consistency.sql
-- Cross-model consistency tests to ensure data integrity across marts

-- Test that artist counts are consistent between models
with artist_counts as (
  select 
    'mart_artist_performance' as model_name,
    count(distinct artist_name) as unique_artists
  from {{ ref('mart_artist_performance') }}
  
  union all
  
  select 
    'staging_reviews' as model_name,
    count(distinct artist_name) as unique_artists
  from {{ ref('stg_pitchfork_reviews') }}
  where not is_artist_missing
)

select *
from (
  select 
    max(unique_artists) - min(unique_artists) as artist_count_difference
  from artist_counts
) 
where artist_count_difference > 0

union all

-- Test that reviewer counts are consistent between models
select *
from (
  with reviewer_counts as (
    select 
      'mart_editorial_dashboard' as model_name,
      count(distinct reviewer) as unique_reviewers
    from {{ ref('mart_editorial_dashboard') }}
    
    union all
    
    select 
      'staging_reviews' as model_name,
      count(distinct reviewer) as unique_reviewers
    from {{ ref('stg_pitchfork_reviews') }}
    where reviewer is not null and trim(reviewer) != ''
  )
  select 
    max(unique_reviewers) - min(unique_reviewers) as reviewer_count_difference
  from reviewer_counts
) 
where reviewer_count_difference > 0

union all

-- Test that total review counts are consistent
select *
from (
  with review_counts as (
    select 
      'mart_artist_performance' as model_name,
      sum(total_reviews) as total_reviews
    from {{ ref('mart_artist_performance') }}
    
    union all
    
    select 
      'mart_editorial_dashboard' as model_name,
      sum(total_reviews) as total_reviews
    from {{ ref('mart_editorial_dashboard') }}
    
    union all
    
    select 
      'staging_reviews' as model_name,
      count(*) as total_reviews
    from {{ ref('stg_pitchfork_reviews') }}
    where not is_artist_missing
  )
  select 
    max(total_reviews) - min(total_reviews) as review_count_difference
  from review_counts
) 
where review_count_difference > 0

union all

-- Test that score ranges are consistent across models
select *
from (
  with score_ranges as (
    select 
      'mart_artist_performance' as model_name,
      min(avg_score) as min_avg_score,
      max(avg_score) as max_avg_score
    from {{ ref('mart_artist_performance') }}
    
    union all
    
    select 
      'mart_editorial_dashboard' as model_name,
      min(avg_score) as min_avg_score,
      max(avg_score) as max_avg_score
    from {{ ref('mart_editorial_dashboard') }}
    
    union all
    
    select 
      'staging_reviews' as model_name,
      min(score) as min_avg_score,
      max(score) as max_avg_score
    from {{ ref('stg_pitchfork_reviews') }}
    where not is_score_missing
  )
  select 
    abs(max(max_avg_score) - min(max_avg_score)) as max_score_difference,
    abs(max(min_avg_score) - min(min_avg_score)) as min_score_difference
  from score_ranges
) 
where max_score_difference > 1 or min_score_difference > 1

union all

-- Test that date ranges are consistent
select *
from (
  with date_ranges as (
    select 
      'mart_artist_performance' as model_name,
      min(first_review_date) as earliest_date,
      max(latest_review_date) as latest_date
    from {{ ref('mart_artist_performance') }}
    
    union all
    
    select 
      'mart_editorial_dashboard' as model_name,
      min(first_review_date) as earliest_date,
      max(latest_review_date) as latest_date
    from {{ ref('mart_editorial_dashboard') }}
    
    union all
    
    select 
      'staging_reviews' as model_name,
      min(review_date) as earliest_date,
      max(review_date) as latest_date
    from {{ ref('stg_pitchfork_reviews') }}
    where not is_artist_missing
  )
  select 
    abs(julianday(max(latest_date)) - julianday(min(latest_date))) as latest_date_difference,
    abs(julianday(max(earliest_date)) - julianday(min(earliest_date))) as earliest_date_difference
  from date_ranges
) 
where latest_date_difference > 7 or earliest_date_difference > 7

