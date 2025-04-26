# variables.tf
variable "credentials" {
  description = "Path to Google Cloud service account credentials"
  type        = string
}

variable "project" {
  description = "Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "Default GCP region"
  type        = string
}