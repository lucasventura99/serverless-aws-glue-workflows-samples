# Sales ETL Workflow Sample

This project demonstrates a multi-stage ETL workflow for sales data using AWS Glue and the Serverless Framework.

## Features
- Daily scheduled workflow
- Data cataloging with Glue Crawler
- Data quality checks
- Data transformation and aggregation

## Quick Start
1. Install dependencies:
   ```bash
   npm install
   ```
2. Configure AWS credentials:
   ```bash
   aws configure
   ```
3. Deploy:
   ```bash
   serverless deploy --stage dev
   ```

## Data Example
```json
{
  "transaction_date": "2024-03-31",
  "transaction_time": "14:30:00",
  "amount": 100.50,
  "product_id": "123",
  "customer_id": "456"
}
```

## Cleanup
```bash
serverless remove --stage dev
```

## License
ISC