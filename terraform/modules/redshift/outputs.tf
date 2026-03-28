output "workgroup_endpoint" {
  description = "Redshift Serverless workgroup endpoint (host:port)"
  value       = "${aws_redshiftserverless_workgroup.main.endpoint[0].address}:${aws_redshiftserverless_workgroup.main.endpoint[0].port}"
}

output "workgroup_name" {
  description = "Redshift Serverless workgroup name"
  value       = aws_redshiftserverless_workgroup.main.workgroup_name
}

output "namespace_name" {
  description = "Redshift Serverless namespace name"
  value       = aws_redshiftserverless_namespace.main.namespace_name
}

output "database_name" {
  description = "Redshift database name"
  value       = aws_redshiftserverless_namespace.main.db_name
}

output "admin_username" {
  description = "Redshift admin username"
  value       = aws_redshiftserverless_namespace.main.admin_username
}

output "s3_iam_role_arn" {
  description = "IAM role ARN Redshift uses to COPY from S3"
  value       = aws_iam_role.redshift_s3.arn
}
