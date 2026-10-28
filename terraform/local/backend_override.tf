# State in Floci's S3. Bucket, key and region still come from -backend-config.
terraform {
  backend "s3" {
    endpoints                   = { s3 = "http://localhost:4566", sts = "http://localhost:4566" }
    use_path_style              = true
    access_key                  = "test"
    secret_key                  = "test"
    skip_credentials_validation = true
    skip_requesting_account_id  = true
    skip_region_validation      = true
    skip_metadata_api_check     = true
  }
}
