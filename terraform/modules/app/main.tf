terraform {
  required_version = ">= 1.10"
  required_providers {
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
  # Bucket, key, and region come from -backend-config. use_lockfile is S3
  # native state locking (Terraform 1.10+); no DynamoDB table needed.
  backend "s3" {
    use_lockfile = true
  }
}

variable "env_name" {
  type = string
}

variable "app_version" {
  type    = string
  default = "v1"
}

variable "idempotency_key" {
  type    = string
  default = ""
}

# Demo hook: when true, the app deploy fails during apply (after network and
# compute exist), so the workflow's saga rollback has something to undo.
variable "simulate_failure" {
  type    = bool
  default = false
}

resource "null_resource" "app_marker" {
  triggers = {
    env         = var.env_name
    app_version = var.app_version
  }

  provisioner "local-exec" {
    command = var.simulate_failure ? "echo 'Simulated app deploy failure' >&2; exit 1" : "true"
  }
}

output "app_version" {
  value = var.app_version
}
