-- test_editorial_dashboard_business_logic.sql
-- Business logic validation tests for editorial dashboard

-- Test that senior reviewers have high review counts
select *
from {{ ref('mart_editorial_dashboard') }}
where reviewer_classification = 'Senior Reviewer' 
  and total_reviews < 100

union all

-- Test that veteran reviewers have long career spans
select *
from {{ ref('mart_editorial_dashboard') }}
where experience_level = 'Veteran' 
  and career_span_years < 5

union all

-- Test that very active reviewers have high recent activity
select *
from {{ ref('mart_editorial_dashboard') }}
where activity_level = 'Very Active' 
  and recent_review_count < 20

union all

-- Test that very generous reviewers have high average scores
select *
from {{ ref('mart_editorial_dashboard') }}
where generosity_level = 'Very Generous' 
  and avg_score < 7.5

union all

-- Test that very critical reviewers have low average scores
select *
from {{ ref('mart_editorial_dashboard') }}
where generosity_level = 'Very Critical' 
  and avg_score > 6.0

union all

-- Test that consistent scorers have low standard deviation
select *
from {{ ref('mart_editorial_dashboard') }}
where scoring_consistency = 'Very Consistent' 
  and score_stddev > 0.8

union all

-- Test that high workload reviewers have high recent activity
select *
from {{ ref('mart_editorial_dashboard') }}
where current_workload = 'High Workload' 
  and recent_review_count < 20

union all

-- Test that inactive reviewers have no recent activity
select *
from {{ ref('mart_editorial_dashboard') }}
where current_workload = 'Inactive' 
  and recent_review_count > 0

union all

-- Test that specialists have high concentration in their genre
select *
from {{ ref('mart_editorial_dashboard') }}
where specialization = 'Rock Specialist' 
  and rock_reviews < (total_reviews * 0.4)

union all

-- Test that generalists don't have high concentration in any genre
select *
from {{ ref('mart_editorial_dashboard') }}
where specialization = 'Generalist' 
  and (
    rock_reviews > (total_reviews * 0.4) or
    pop_electronic_reviews > (total_reviews * 0.4) or
    jazz_blues_reviews > (total_reviews * 0.4) or
    hip_hop_rb_reviews > (total_reviews * 0.4) or
    folk_country_reviews > (total_reviews * 0.4) or
    experimental_reviews > (total_reviews * 0.4)
  )

union all

-- Test that high performers have good performance scores
select *
from {{ ref('mart_editorial_dashboard') }}
where performance_tier = 'High Performer' 
  and overall_performance_score < 8.0

union all

-- Test that needs attention reviewers have poor performance scores
select *
from {{ ref('mart_editorial_dashboard') }}
where performance_tier = 'Needs Attention' 
  and overall_performance_score >= 4.0

union all

-- Test that risk assessments align with flags
select *
from {{ ref('mart_editorial_dashboard') }}
where risk_assessment = 'High Risk - Low Activity' 
  and not is_occasional_reviewer

union all

select *
from {{ ref('mart_editorial_dashboard') }}
where risk_assessment = 'Medium Risk - Limited Scope' 
  and not is_single_artist_reviewer

union all

-- Test that team metrics are consistent across all reviewers
with team_metrics_check as (
  select 
    count(distinct total_reviewers) as unique_team_sizes,
    count(distinct team_avg_score) as unique_team_scores,
    count(distinct avg_team_experience) as unique_team_experiences
  from {{ ref('mart_editorial_dashboard') }}
)
select *
from team_metrics_check
where unique_team_sizes > 1 
   or unique_team_scores > 1 
   or unique_team_experiences > 1

