import os
import pytest

# Set dummy AWS credentials before any module imports boto3, preventing
# botocore from falling through to the login credential provider.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
