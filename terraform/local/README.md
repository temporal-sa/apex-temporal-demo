# Local (Floci) overrides

Copied into each workspace only for the `local` target, next to the module's
own `main.tf`. Terraform merges `*_override.tf` files into the module, so the
AWS modules in `../modules/` stay exactly as they run against real AWS.

| File | Copied into | Why |
|---|---|---|
| `backend_override.tf` | every module | S3 state in Floci's S3 instead of AWS |
| `aws_provider_override.tf` | modules with an `aws` provider | AWS API calls go to Floci |
| `compute_override.tf` | compute | Floci-specific instance settings (see comments) |

`http://localhost:4566` is replaced with `$FLOCI_ENDPOINT` when copied.
Tested against Floci 2.2.0; 1.5.x has broken DescribeTags and
DescribeRouteTables filters that hang or fail the AWS provider.
