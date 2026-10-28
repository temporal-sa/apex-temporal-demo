import os
from dotenv import load_dotenv

load_dotenv()

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TASK_QUEUE_NONPROD = os.getenv("TEMPORAL_TASK_QUEUE_NONPROD", "nonprod-provisioning")

# Terraform remote state. The bucket must already exist; see README.
# Note: provisioned AWS resources always land in us-east-1 (hardcoded in the
# Terraform modules). TF_STATE_REGION is only where the state bucket lives.
TF_STATE_BUCKET = os.getenv("TF_STATE_BUCKET", "")
TF_STATE_REGION = os.getenv("TF_STATE_REGION", "us-east-1")

# The `local` target: Terraform against Floci, a local AWS emulator. Its state
# bucket lives inside Floci; the local worker creates it on startup.
FLOCI_ENDPOINT = os.getenv("FLOCI_ENDPOINT", "http://localhost:4566")
LOCAL_STATE_BUCKET = os.getenv("LOCAL_STATE_BUCKET", "apex-local-state")
