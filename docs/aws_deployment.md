# AWS deployment design

**Not deployed or verified.** The working Kafka-compatible/Spark/Airflow/inventory
platform runs in local Docker. This Terraform does not provision Airflow, its metadata
database, daily-close storage/coordination, or the inventory/application services.
It is not an automated cloud deployment of the current repository.

The Terraform under `infrastructure/terraform` creates a cost-conscious analytical foundation:

- one versioned, encrypted S3 bucket with `bronze/`, `silver/`, `gold/`, `reports/`, and `athena-results/` prefixes;
- an AWS Glue Data Catalog database;
- an Athena workgroup with encrypted query results and per-query scan controls;
- an EMR Serverless Spark application and least-privilege execution role;
- a CloudWatch log group with bounded retention;
- optional MSK Serverless, disabled by default because an always-on streaming layer can materially change cost.

## Environment separation

Use separate state and `project_name` values for development and production. The generated bucket name includes account and region, but a production organization should also use remote state with locking, tagging policies, permissions boundaries, and a CI role authenticated through GitHub OIDC.

## Proposed execution flow

1. Ingestion writes immutable, date-partitioned objects to `bronze/`.
2. EMR Serverless validates schemas, records bad inputs under `reports/quarantine/`, and writes conformed Parquet to `silver/`.
3. Gold jobs create decision tables partitioned for common Athena predicates.
4. Glue crawlers or explicit table definitions register schemas; explicit definitions are preferred for strict production contracts.
5. Athena supports exploratory analysis while serving workloads read curated objects through an API or warehouse.

## Security and operations

The bucket blocks all public access, requires TLS, enables server-side encryption, and versions objects. The execution policy is prefix-scoped to the project bucket and log group. CloudTrail data events, KMS customer-managed keys, VPC endpoints, Lake Formation grants, and Macie classification are recommended production additions.

The module creates infrastructure but does not automatically submit a job or upload customer data. Review `terraform plan`, current regional pricing, IAM policy requirements, and retention choices before applying.

## Gaps before deploying the retail platform

Replace shared local filesystem locks and atomic renames with storage-appropriate
coordination; choose a managed Airflow deployment and persistent metadata store; package
the application and Spark jobs; configure secrets, authentication, observability,
retention, networking, and replay/backfill controls. Validate recovery and cost separately.
No AWS resource creation is required for the local tests or GitHub Actions workflows.
