variable "project_name" {
  description = "Lowercase project/environment identifier used in resource names."
  type        = string
  default     = "merchmind-dev"

  validation {
    condition     = can(regex("^[a-z0-9-]{3,32}$", var.project_name))
    error_message = "project_name must contain 3-32 lowercase letters, numbers, or hyphens."
  }
}

variable "aws_region" {
  description = "AWS region for the analytical platform."
  type        = string
  default     = "us-east-1"
}

variable "log_retention_days" {
  description = "CloudWatch retention for Spark job logs."
  type        = number
  default     = 30
}

variable "enable_msk_serverless" {
  description = "Provision MSK Serverless. Disabled by default to avoid streaming costs."
  type        = bool
  default     = false
}

variable "vpc_id" {
  description = "VPC for optional MSK Serverless. Required when enable_msk_serverless is true."
  type        = string
  default     = ""
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for optional MSK Serverless."
  type        = list(string)
  default     = []
}
