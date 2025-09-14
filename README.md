# Pitchfork Music Reviews Analytics

A dbt project for analyzing Pitchfork music review data to generate insights into music trends, artist performance, and review patterns.

## Project Structure

```
pitch/
├── docs/                    # Documentation
│   └── PRD.md              # Product Requirements Document
├── models/                 # dbt models
│   ├── staging/            # Raw data transformations
│   ├── intermediate/       # Business logic transformations
│   └── marts/              # Final business entities
├── macros/                 # Reusable SQL logic
├── tests/                  # Data quality tests
├── seeds/                  # Reference data
├── snapshots/              # Slowly changing dimensions
├── .cursorrules           # Cursor IDE rules for dbt
├── dbt_project.yml        # dbt project configuration
├── packages.yml           # dbt packages
└── README.md              # This file
```

## Getting Started

### Prerequisites
- dbt CLI installed
- SQLite database with Pitchfork review data
- Python 3.7+

### Setup
1. Install dbt dependencies:
   ```bash
   dbt deps
   ```

2. Configure your database connection in `profiles.yml`

3. Run the project:
   ```bash
   dbt run
   ```

4. Run tests:
   ```bash
   dbt test
   ```

5. Generate documentation:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## Key Features

- **Staging Layer**: Clean and standardize raw review data
- **Intermediate Layer**: Calculate business metrics and trends
- **Marts Layer**: Provide analytics-ready datasets
- **Data Quality**: Comprehensive testing and validation
- **Documentation**: Self-documenting models and business context

## Data Sources

- **Primary**: Pitchfork reviews database (SQLite)
- **Future**: External music metadata, social sentiment

## Business Use Cases

- Track artist performance over time
- Analyze genre trends and evolution
- Identify emerging artists and trends
- Support editorial decision-making
- Enable self-service analytics

## Development Guidelines

See `.cursorrules` for detailed development guidelines and conventions.

## Documentation

- **PRD**: See `docs/PRD.md` for complete product requirements
- **dbt Docs**: Run `dbt docs serve` for model documentation
- **Code Comments**: Inline documentation in SQL models

## Contributing

1. Follow the naming conventions in `.cursorrules`
2. Add tests for new models
3. Update documentation for changes
4. Ensure all tests pass before submitting

## Support

For questions or issues, refer to:
- dbt Documentation: https://docs.getdbt.com/
- Project PRD: `docs/PRD.md`
- Model documentation: `dbt docs serve`
