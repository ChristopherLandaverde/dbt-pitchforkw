-- mart_editorial_dashboard.sql
-- Editorial team metrics and reviewer analysis for management insights

{{ config(materialized='table') }}

with reviewer_performance as (
  select
    reviewer,
    total_reviews,
    first_review_date,
    latest_review_date,
    avg_score,
    min_score,
    max_score,
    score_stddev,
    median_score,
    best_new_music_count,
    best_new_music_percentage,
    career_span_days,
    unique_artists_reviewed,
    recent_avg_score,
    recent_review_count,
    score_trend,
    experience_level,
    activity_level,
    scoring_consistency,
    generosity_level,
    rock_reviews,
    pop_electronic_reviews,
    jazz_blues_reviews,
    hip_hop_rb_reviews,
    folk_country_reviews,
    experimental_reviews,
    career_span_years,
    artist_diversity_percentage,
    is_occasional_reviewer,
    is_single_artist_reviewer
    
  from {{ ref('int_reviewer_metrics') }}
),

-- Reviewer rankings and performance analysis
reviewer_rankings as (
  select
    *,
    
    -- Performance rankings
    row_number() over (order by total_reviews desc) as rank_by_activity,
    row_number() over (order by avg_score desc) as rank_by_avg_score,
    row_number() over (order by best_new_music_count desc) as rank_by_bnm_count,
    row_number() over (order by unique_artists_reviewed desc) as rank_by_diversity,
    
    -- Performance percentiles
    ntile(10) over (order by total_reviews desc) as activity_percentile,
    ntile(10) over (order by avg_score desc) as score_percentile,
    ntile(10) over (order by best_new_music_count desc) as bnm_percentile,
    ntile(10) over (order by unique_artists_reviewed desc) as diversity_percentile,
    
    -- Reviewer classification
    case 
      when total_reviews >= 100 and avg_score >= 7.0 then 'Senior Reviewer'
      when total_reviews >= 50 and avg_score >= 6.5 then 'Experienced Reviewer'
      when total_reviews >= 20 then 'Mid-level Reviewer'
      when total_reviews >= 5 then 'Junior Reviewer'
      else 'Occasional Reviewer'
    end as reviewer_classification,
    
    -- Workload assessment
    case 
      when recent_review_count >= 20 then 'High Workload'
      when recent_review_count >= 10 then 'Moderate Workload'
      when recent_review_count >= 5 then 'Light Workload'
      when recent_review_count >= 1 then 'Minimal Workload'
      else 'Inactive'
    end as current_workload,
    
    -- Specialization analysis
    case 
      when rock_reviews > (total_reviews * 0.4) then 'Rock Specialist'
      when pop_electronic_reviews > (total_reviews * 0.4) then 'Pop/Electronic Specialist'
      when jazz_blues_reviews > (total_reviews * 0.4) then 'Jazz/Blues Specialist'
      when hip_hop_rb_reviews > (total_reviews * 0.4) then 'Hip-Hop/R&B Specialist'
      when folk_country_reviews > (total_reviews * 0.4) then 'Folk/Country Specialist'
      when experimental_reviews > (total_reviews * 0.4) then 'Experimental Specialist'
      else 'Generalist'
    end as specialization,
    
    -- Risk assessment
    case 
      when is_occasional_reviewer = true then 'High Risk - Low Activity'
      when is_single_artist_reviewer = true then 'Medium Risk - Limited Scope'
      when score_stddev > 2.0 then 'Medium Risk - Inconsistent Scoring'
      when recent_review_count = 0 and career_span_years > 1 then 'Medium Risk - Recently Inactive'
      when avg_score < 5.0 then 'Low Risk - Critical Reviewer'
      else 'Low Risk'
    end as risk_assessment,
    
    -- Performance score (composite metric)
    round(
      (activity_percentile * 0.3) +
      (score_percentile * 0.25) +
      (bnm_percentile * 0.25) +
      (diversity_percentile * 0.2), 1
    ) as overall_performance_score
    
  from reviewer_performance
),

-- Team-level metrics
team_metrics as (
  select
    count(*) as total_reviewers,
    avg(total_reviews) as avg_reviews_per_reviewer,
    avg(avg_score) as team_avg_score,
    sum(best_new_music_count) as total_team_bnm_count,
    avg(best_new_music_percentage) as team_avg_bnm_percentage,
    avg(unique_artists_reviewed) as avg_artists_per_reviewer,
    avg(artist_diversity_percentage) as team_avg_diversity,
    avg(score_stddev) as team_avg_consistency,
    avg(career_span_years) as avg_team_experience,
    
    -- Team composition
    sum(case when experience_level = 'Veteran' then 1 else 0 end) as veteran_reviewers,
    sum(case when experience_level = 'Experienced' then 1 else 0 end) as experienced_reviewers,
    sum(case when experience_level = 'Mid-level' then 1 else 0 end) as midlevel_reviewers,
    sum(case when experience_level = 'Junior' then 1 else 0 end) as junior_reviewers,
    sum(case when experience_level = 'New' then 1 else 0 end) as new_reviewers,
    
    -- Activity distribution
    sum(case when activity_level = 'Very Active' then 1 else 0 end) as very_active_reviewers,
    sum(case when activity_level = 'Active' then 1 else 0 end) as active_reviewers,
    sum(case when activity_level = 'Moderate' then 1 else 0 end) as moderate_reviewers,
    sum(case when activity_level = 'Low Activity' then 1 else 0 end) as low_activity_reviewers,
    
    -- Generosity distribution
    sum(case when generosity_level = 'Very Generous' then 1 else 0 end) as very_generous_reviewers,
    sum(case when generosity_level = 'Generous' then 1 else 0 end) as generous_reviewers,
    sum(case when generosity_level = 'Moderate' then 1 else 0 end) as moderate_reviewers,
    sum(case when generosity_level = 'Critical' then 1 else 0 end) as critical_reviewers,
    sum(case when generosity_level = 'Very Critical' then 1 else 0 end) as very_critical_reviewers
    
  from reviewer_performance
),

-- Workload distribution analysis
workload_analysis as (
  select
    current_workload,
    count(*) as reviewer_count,
    round(count(*) * 100.0 / sum(count(*)) over (), 1) as percentage_of_team,
    avg(total_reviews) as avg_total_reviews,
    avg(recent_review_count) as avg_recent_reviews,
    avg(overall_performance_score) as avg_performance_score
    
  from reviewer_rankings
  group by current_workload
),

-- Specialization analysis
specialization_analysis as (
  select
    specialization,
    count(*) as reviewer_count,
    round(count(*) * 100.0 / sum(count(*)) over (), 1) as percentage_of_team,
    avg(total_reviews) as avg_total_reviews,
    avg(avg_score) as avg_score_in_specialization,
    avg(best_new_music_percentage) as avg_bnm_rate_in_specialization
    
  from reviewer_rankings
  group by specialization
),

final as (
  select
    rp.reviewer,
    
    -- Core Performance Metrics
    rp.total_reviews,
    rp.avg_score,
    rp.best_new_music_count,
    rp.best_new_music_percentage,
    rp.unique_artists_reviewed,
    rp.artist_diversity_percentage,
    
    -- Career Timeline
    rp.first_review_date,
    rp.latest_review_date,
    rp.career_span_years,
    rp.recent_review_count,
    rp.recent_avg_score,
    
    -- Score Analysis
    rp.min_score,
    rp.max_score,
    rp.score_stddev,
    rp.median_score,
    rp.score_trend,
    
    -- Classifications
    rr.reviewer_classification,
    rp.experience_level,
    rp.activity_level,
    rp.scoring_consistency,
    rp.generosity_level,
    rr.current_workload,
    rr.specialization,
    rr.risk_assessment,
    
    -- Rankings and Percentiles
    rr.rank_by_activity,
    rr.rank_by_avg_score,
    rr.rank_by_bnm_count,
    rr.rank_by_diversity,
    rr.activity_percentile,
    rr.score_percentile,
    rr.bnm_percentile,
    rr.diversity_percentile,
    rr.overall_performance_score,
    
    -- Specialization Details
    rp.rock_reviews,
    rp.pop_electronic_reviews,
    rp.jazz_blues_reviews,
    rp.hip_hop_rb_reviews,
    rp.folk_country_reviews,
    rp.experimental_reviews,
    
    -- Team Context
    tm.total_reviewers,
    tm.avg_reviews_per_reviewer,
    tm.team_avg_score,
    tm.team_avg_bnm_percentage,
    tm.team_avg_diversity,
    tm.avg_team_experience,
    
    -- Performance vs Team
    round(rp.total_reviews - tm.avg_reviews_per_reviewer, 1) as reviews_vs_team_avg,
    round(rp.avg_score - tm.team_avg_score, 2) as score_vs_team_avg,
    round(rp.best_new_music_percentage - tm.team_avg_bnm_percentage, 1) as bnm_vs_team_avg,
    round(rp.artist_diversity_percentage - tm.team_avg_diversity, 1) as diversity_vs_team_avg,
    
    -- Management Insights
    case 
      when rr.overall_performance_score >= 8.0 then 'High Performer'
      when rr.overall_performance_score >= 6.0 then 'Good Performer'
      when rr.overall_performance_score >= 4.0 then 'Average Performer'
      else 'Needs Attention'
    end as performance_tier,
    
    case 
      when rr.current_workload = 'High Workload' and rr.overall_performance_score < 6.0 then 'Overworked - Consider Reducing Load'
      when rr.current_workload = 'Inactive' and rp.career_span_years > 1 then 'Inactive - Follow Up Required'
      when rr.risk_assessment != 'Low Risk' then 'Risk Management - Monitor Closely'
      when rr.overall_performance_score >= 8.0 then 'Star Performer - Recognize Excellence'
      else 'Standard Management'
    end as management_action,
    
    -- Development Recommendations
    case 
      when rp.experience_level = 'New' then 'Provide Mentorship and Training'
      when rp.experience_level = 'Junior' and rr.overall_performance_score >= 6.0 then 'Ready for More Challenging Assignments'
      when rp.specialization != 'Generalist' and rr.overall_performance_score >= 7.0 then 'Consider Cross-Genre Training'
      when rr.overall_performance_score < 4.0 then 'Performance Improvement Plan'
      else 'Continue Current Development Path'
    end as development_recommendation,
    
    -- Data Quality Flags
    rp.is_occasional_reviewer,
    rp.is_single_artist_reviewer,
    
    -- Last Updated
    current_timestamp as last_updated
    
  from reviewer_performance rp
  left join reviewer_rankings rr on rp.reviewer = rr.reviewer
  cross join team_metrics tm
)

select * from final
order by overall_performance_score desc, total_reviews desc

