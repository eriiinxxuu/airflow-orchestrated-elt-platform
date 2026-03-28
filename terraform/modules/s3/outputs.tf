output "mwaa_bucket_name" {
  description = "S3 bucket name for MWAA DAGs, plugins and requirements"
  value       = aws_s3_bucket.mwaa.bucket
}

output "mwaa_bucket_arn" {
  description = "S3 bucket ARN for MWAA"
  value       = aws_s3_bucket.mwaa.arn
}

output "data_bucket_name" {
  description = "S3 bucket name for raw ECS extractor data"
  value       = aws_s3_bucket.data.bucket
}

output "data_bucket_arn" {
  description = "S3 bucket ARN for raw ECS extractor data"
  value       = aws_s3_bucket.data.arn
}

output "plugins_s3_object_version" {
  description = "Version ID of the uploaded plugins.zip — passed to MWAA depends_on"
  value       = aws_s3_object.plugins_zip.version_id
}

output "requirements_s3_object_version" {
  description = "Version ID of the uploaded requirements.txt — passed to MWAA depends_on"
  value       = aws_s3_object.requirements_txt.version_id
}
