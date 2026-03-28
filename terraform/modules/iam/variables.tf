variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "mwaa_env_arn" {
  description = "ARN of the MWAA environment the Lambda is allowed to generate tokens for"
  type        = string
}

variable "github_org" {
  description = "GitHub username or organisation, e.g. 'xuxinying'"
  type        = string
}

variable "github_repo" {
  description = "GitHub repository name, e.g. 'yf-elt-platform'"
  type        = string
}
