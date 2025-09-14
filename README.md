# 🎵 Pitchfork Music Reviews Analytics

I chose Pitchfork review data because it represents the kind of **messy, real-world dataset** that data engineers encounter daily:
- Inconsistent text fields and naming conventions
- Missing values and data quality issues  
- Complex relationships requiring thoughtful modeling
- Multiple business stakeholder needs (editorial, management, analysts

This project demonstrates my ability to take raw, unstructured data and transform it into clean, reliable, business-ready datasets using modern data engineering practices.

## 🎯 Project Overview

This project demonstrates advanced **data engineering** and **analytics engineering** skills by building a complete ETL pipeline that processes music review data into business-ready analytics tables. The solution showcases expertise in **dbt**, **SQL**, **data modeling**, and **data quality engineering**.

## 🏗️ Architecture & Technical Stack

### **Data Pipeline Architecture**
```
Raw Data → Staging Layer → Intermediate Layer → Marts Layer → Business Insights
```

### **Technology Stack**
- **dbt Core** - Data transformation and modeling
- **SQLite** - Database engine
- **SQL** - Data transformation logic
- **YAML** - Configuration and documentation
- **Git** - Version control and collaboration

## 📊 What I Accomplished

### **1. Built a Complete Data Pipeline**
- **4 Staging Models**: Clean and standardize raw review data
- **4 Intermediate Models**: Calculate business metrics and trends
- **4 Mart Tables**: Provide analytics-ready datasets for business users

### **2. Implemented Advanced Data Modeling**
- **Dimensional Modeling**: Star schema design for optimal query performance
- **Data Layering**: Proper separation of concerns across staging, intermediate, and marts
- **Business Logic**: Complex calculations for artist performance, genre trends, and editorial insights

### **3. Created Comprehensive Data Quality Framework**
- **6 Custom Tests**: Business rule validation and data consistency checks
- **287 Total Tests**: Comprehensive coverage across all models
- **Data Validation**: Ensures data integrity and business logic compliance

### **4. Delivered Business Value**
- **Artist Performance Analytics**: Track artist success metrics over time
- **Genre Trend Analysis**: Identify emerging and declining music genres
- **Editorial Dashboard**: Support editorial team decision-making
- **Review Insights**: Analyze review patterns and score distributions

## 📁 Project Structure

```
dbt-pitchforkw/
├── dbt_project.yml              # dbt configuration
├── packages.yml                 # dbt packages
├── profiles.yml                 # Database connection
├── README.md                    # Project documentation
├── docs/
│   └── PRD.md                  # Product Requirements Document
├── macros/
│   └── get_standardized_genre.sql  # Reusable SQL logic
├── models/
│   ├── staging/
│   │   ├── stg_pitchfork_reviews.sql
│   │   └── schema.yml
│   ├── intermediate/
│   │   ├── int_artist_metrics.sql
│   │   ├── int_genre_trends.sql
│   │   ├── int_reviewer_metrics.sql
│   │   ├── int_score_trends.sql
│   │   └── schema.yml
│   └── marts/
│       ├── mart_artist_performance.sql
│       ├── mart_editorial_dashboard.sql
│       ├── mart_genre_analysis.sql
│       ├── mart_review_insights.sql
│       └── _marts__models.yml
└── tests/generic/
    ├── test_artist_business_logic.sql
    ├── test_cross_model_consistency.sql
    ├── test_editorial_dashboard_business_logic.sql
    ├── test_genre_business_logic.sql
    ├── test_mart_data_quality.sql
    └── test_review_insights_business_logic.sql
```

## 🚀 Key Features & Capabilities

### **Data Transformation Pipeline**
- **Raw Data Processing**: Clean and standardize inconsistent review data
- **Business Logic Implementation**: Calculate complex metrics like artist success rates, genre health scores
- **Data Quality Assurance**: Comprehensive testing and validation framework
- **Documentation**: Self-documenting models with business context

### **Analytics-Ready Mart Tables**
1. **`mart_artist_performance`**: Artist success metrics, review trends, and performance indicators
2. **`mart_editorial_dashboard`**: Reviewer performance, editorial team metrics, and management insights
3. **`mart_genre_analysis`**: Genre trend analysis, health metrics, and evolution tracking
4. **`mart_review_insights`**: Review pattern analysis, score distributions, and editorial insights

### **Advanced SQL Techniques**
- **Window Functions**: For calculating running averages and rankings
- **CTEs**: Complex business logic with readable, maintainable code
- **Data Type Handling**: Proper handling of dates, scores, and categorical data
- **Performance Optimization**: Efficient joins and aggregations

## 🛠️ Technical Skills Demonstrated

### **Data Engineering**
- **ETL Pipeline Design**: End-to-end data processing workflow
- **Data Modeling**: Dimensional modeling and star schema design
- **Data Quality**: Comprehensive testing and validation framework
- **Performance Optimization**: Efficient SQL queries and data processing

### **dbt Expertise**
- **Model Configuration**: Proper materialization strategies and configurations
- **Macros**: Reusable SQL logic and DRY principles
- **Testing**: Custom tests for business logic validation
- **Documentation**: Self-documenting models with business context

### **SQL Mastery**
- **Complex Queries**: Multi-table joins, window functions, and aggregations
- **Business Logic**: Implementing complex calculations and metrics
- **Data Transformation**: Cleaning, standardizing, and enriching data
- **Performance**: Optimized queries for large datasets

## 📈 Business Impact

### **Editorial Team**
- Track reviewer performance and consistency
- Identify emerging artists and trends
- Support editorial decision-making with data

### **Music Industry**
- Analyze genre evolution and market trends
- Track artist success patterns over time
- Understand review score distributions and patterns

### **Data Team**
- Self-service analytics capabilities
- Consistent, reliable data sources
- Comprehensive data quality assurance

## 🚀 Getting Started

### Prerequisites
- dbt CLI installed
- SQLite database with Pitchfork review data
- Python 3.7+

### Setup
1. **Install dependencies**:
   ```bash
   dbt deps
   ```

2. **Configure database** in `profiles.yml`

3. **Run the pipeline**:
   ```bash
   dbt run
   ```

4. **Run tests**:
   ```bash
   dbt test
   ```

5. **Generate documentation**:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## 🎯 Portfolio Highlights

This project demonstrates:
- **End-to-end data engineering** from raw data to business insights
- **Advanced dbt skills** including modeling, testing, and documentation
- **SQL expertise** with complex business logic and performance optimization
- **Data quality engineering** with comprehensive testing framework
- **Business acumen** by delivering actionable analytics for music industry

## 📚 Documentation

- **PRD**: See `docs/PRD.md` for complete product requirements
- **dbt Docs**: Run `dbt docs serve` for interactive model documentation
- **Code Comments**: Inline documentation in all SQL models

## 🔗 Repository

[GitHub Repository](https://github.com/ChristopherLandaverde/dbt-pitchforkw)

---

*This project showcases advanced data engineering skills and demonstrates the ability to build production-ready analytics solutions using modern data tools and best practices.*
