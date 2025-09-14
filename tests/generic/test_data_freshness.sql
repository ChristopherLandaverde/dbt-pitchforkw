-- test_data_freshness.sql
-- Data freshness tests to ensure marts are up-to-date

-- Test that all marts have been updated recently (within last 24 hours)
select 
  'mart_artist_performance' as model_name,
  last_updated
from {{ ref('mart_artist_performance') }}
where julianday('now') - julianday(last_updated) > 1

union all

select 
  'mart_genre_analysis' as model_name,
  last_updated
from {{ ref('mart_genre_analysis') }}
where julianday('now') - julianday(last_updated) > 1

union all

select 
  'mart_review_insights' as model_name,
  last_updated
from {{ ref('mart_review_insights') }}
where julianday('now') - julianday(last_updated) > 1

union all

select 
  'mart_editorial_dashboard' as model_name,
  last_updated
from {{ ref('mart_editorial_dashboard') }}
where julianday('now') - julianday(last_updated) > 1

union all

-- Test that latest review dates in marts are recent (within last 30 days)
select 
  'mart_artist_performance' as model_name,
  max(latest_review_date) as latest_review_date
from {{ ref('mart_artist_performance') }}
where julianday('now') - julianday(max(latest_review_date)) > 30

union all

select 
  'mart_editorial_dashboard' as model_name,
  max(latest_review_date) as latest_review_date
from {{ ref('mart_editorial_dashboard') }}
where julianday('now') - julianday(max(latest_review_date)) > 30

union all

-- Test that recent review counts are reasonable (not all zeros)
select 
  'mart_artist_performance' as model_name,
  count(*) as artists_with_no_recent_reviews
from {{ ref('mart_artist_performance') }}
where recent_review_count = 0
having count(*) > (select count(*) * 0.9 from {{ ref('mart_artist_performance') }})

union all

select 
  'mart_editorial_dashboard' as model_name,
  count(*) as reviewers_with_no_recent_reviews
from {{ ref('mart_editorial_dashboard') }}
where recent_review_count = 0
having count(*) > (select count(*) * 0.8 from {{ ref('mart_editorial_dashboard') }})

