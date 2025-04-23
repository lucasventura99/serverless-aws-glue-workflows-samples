# Comprehensive Sales ETL Workflow Sample

This project demonstrates advanced usage of the `serverless-aws-glue-workflows` plugin by implementing a complex ETL workflow that processes sales data through multiple stages using AWS Glue.

## Project Structure

```
.
├── README.md
├── package.json
├── serverless.yml
└── scripts/
    ├── data-quality.py
    ├── transform.py
    └── aggregate.py
```

## Overview

This sample project implements a comprehensive ETL workflow that:
1. Uses a scheduled trigger to start the workflow daily
2. Runs a crawler to catalog raw sales data
3. Performs data quality checks
4. Transforms the data with business logic
5. Creates multiple aggregations for analysis

## Workflow Components

### Trigger
- Name: `daily-sales-trigger`
- Type: SCHEDULED
- Schedule: Daily at 1 AM
- Starts the workflow by running the data quality check

### Glue Crawler
- Name: `sales-data-crawler`
- Runs daily at 1 AM
- Catalogs data from the `raw-data` S3 bucket
- Updates the `sales_database` in the Glue catalog
- Configures partition and table update behaviors

### Data Quality Job
- Name: `data-quality-check`
- Validates data against quality rules
- Generates quality reports
- Filters out invalid records
- Depends on crawler completion

### Transformation Job
- Name: `transform-sales-data`
- Adds derived fields:
  - Date components (year, month, day of week)
  - Time components (hour)
  - Business logic flags (is_weekend, is_business_hour)
  - Sales categories (small, medium, large)
- Depends on data quality check

### Aggregation Job
- Name: `aggregate-sales`
- Creates multiple aggregations:
  - Daily sales metrics
  - Monthly sales metrics
  - Category-based metrics
  - Time-based metrics
- Depends on transformation job

## Prerequisites

- AWS Account with appropriate permissions
- AWS CLI configured with your credentials
- Node.js and npm installed
- Serverless Framework CLI installed globally (`npm install -g serverless`)

## Setup

1. Install dependencies:
   ```bash
   npm install
   ```

2. Configure AWS credentials:
   ```bash
   aws configure
   ```

3. Create quality rules file:
   ```bash
   aws s3 cp config/quality-rules.json s3://${service}-${stage}/config/quality-rules.json
   ```

4. Deploy the workflow:
   ```bash
   serverless deploy --stage dev
   ```

## Sample Data Structure

The workflow expects input data in the following format:
```json
{
  "transaction_date": "2024-03-31",
  "transaction_time": "14:30:00",
  "amount": 100.50,
  "product_id": "123",
  "customer_id": "456"
}
```

## Quality Rules

The data quality job uses rules defined in `config/quality-rules.json`:
```json
[
  {
    "type": "not_null",
    "column": "amount"
  },
  {
    "type": "date_format",
    "column": "transaction_date",
    "format": "yyyy-MM-dd"
  },
  {
    "type": "value_range",
    "column": "amount",
    "min": 0,
    "max": 1000000
  }
]
```

## Output Tables

The workflow creates the following tables in the Glue catalog:
- `sales_data`: Raw sales data
- `transformed_sales`: Transformed data with derived fields
- `daily_sales`: Daily aggregated metrics
- `monthly_sales`: Monthly aggregated metrics
- `category_sales`: Category-based metrics
- `time_sales`: Time-based metrics

## Monitoring

You can monitor the workflow execution in the AWS Console:
1. Go to AWS Glue Console
2. Navigate to Workflows
3. Select the `salesETLWorkflow` workflow
4. View the execution history and status

## Cleanup

To remove all deployed resources:
```bash
serverless remove --stage dev
```

## License

ISC 