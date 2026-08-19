output "data_lake_bucket" {
  description = "S3 bucket containing all medallion layers."
  value       = aws_s3_bucket.lake.id
}

output "glue_database" {
  description = "Glue catalog database for analytical tables."
  value       = aws_glue_catalog_database.analytics.name
}

output "athena_workgroup" {
  description = "Governed Athena workgroup."
  value       = aws_athena_workgroup.analytics.name
}

output "emr_serverless_application_id" {
  description = "Spark application ID."
  value       = aws_emrserverless_application.spark.id
}

output "emr_execution_role_arn" {
  description = "Role to pass when submitting Spark jobs."
  value       = aws_iam_role.emr_execution.arn
}

output "msk_bootstrap_brokers" {
  description = "MSK Serverless IAM bootstrap brokers when streaming is enabled."
  value       = var.enable_msk_serverless ? aws_msk_serverless_cluster.events[0].bootstrap_brokers_sasl_iam : null
}
