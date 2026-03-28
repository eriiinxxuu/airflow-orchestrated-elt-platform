variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "recovery_window_in_days" {
  description = "Number of days before a deleted secret is permanently removed"
  type        = number
  default     = 7
}
