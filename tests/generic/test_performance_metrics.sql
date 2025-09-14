-- test_performance_metrics.sql
-- Performance and efficiency tests for marts

-- Test that performance scores are well-distributed (not all the same)
select 
  'mart_artist_performance' as model_name,
  'overall_success_score' as metric_name,
  count(distinct overall_success_score) as unique_values,
  count(*) as total_records
from {{ ref('mart_artist_performance') }}
having count(distinct overall_success_score) < 5

union all

select 
  'mart_genre_analysis' as model_name,
  'editorial_priority_score' as metric_name,
  count(distinct editorial_priority_score) as unique_values,
  count(*) as total_records
from {{ ref('mart_genre_analysis') }}
having count(distinct editorial_priority_score) < 5

union all

select 
  'mart_review_insights' as model_name,
  'editorial_kpi_score' as metric_name,
  count(distinct editorial_kpi_score) as unique_values,
  count(*) as total_records
from {{ ref('mart_review_insights') }}
having count(distinct editorial_kpi_score) < 5

union all

select 
  'mart_editorial_dashboard' as model_name,
  'overall_performance_score' as metric_name,
  count(distinct overall_performance_score) as unique_values,
  count(*) as total_records
from {{ ref('mart_editorial_dashboard') }}
having count(distinct overall_performance_score) < 5

union all

-- Test that rankings are properly distributed
select 
  'mart_artist_performance' as model_name,
  'rank_by_avg_score' as metric_name,
  max(rank_by_avg_score) as max_rank,
  count(distinct rank_by_avg_score) as unique_ranks,
  count(*) as total_records
from {{ ref('mart_artist_performance') }}
having max_rank != count(*) or count(distinct rank_by_avg_score) != count(*)

union all

-- Test that percentiles are well-distributed
select 
  'mart_editorial_dashboard' as model_name,
  'activity_percentile' as metric_name,
  count(distinct activity_percentile) as unique_values
from {{ ref('mart_editorial_dashboard') }}
having count(distinct activity_percentile) < 8  -- Should have most of 1-10

union all

-- Test that there are no duplicate rankings
select 
  'mart_genre_analysis' as model_name,
  'current_count_rank' as metric_name,
  current_count_rank,
  count(*) as rank_count
from {{ ref('mart_genre_analysis') }}
group by current_count_rank
having count(*) > 1

union all

-- Test that score distributions are reasonable
select 
  'mart_artist_performance' as model_name,
  'avg_score' as metric_name,
  avg(avg_score) as mean_score,
  count(case when avg_score >= 8.0 then 1 end) * 100.0 / count(*) as high_score_pct
from {{ ref('mart_artist_performance') }}
having avg(avg_score) < 5.0 or avg(avg_score) > 9.0  -- Mean should be reasonable
   or count(case when avg_score >= 8.0 then 1 end) * 100.0 / count(*) > 50  -- Not too many high scores

