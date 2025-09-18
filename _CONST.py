"""
Configuration constants for WiROS Data Ingestion Server
"""

# AWS Configuration
AWS_REGION = "us-east-1"  # Change to your preferred region
S3_BUCKET_NAME = "wiros-csi-data"  # Change to your S3 bucket name
DYNAMODB_TABLE_NAME = "wiros-metadata"  # Change to your DynamoDB table name

# S3 Configuration
S3_KEY_PREFIX = "csi-data/"  # Prefix for S3 object keys
S3_UPLOAD_TIMEOUT = 300  # Timeout for S3 uploads in seconds

# DynamoDB Configuration
DYNAMODB_READ_CAPACITY = 5
DYNAMODB_WRITE_CAPACITY = 5

# Server Configuration
DEFAULT_HOST = "137.110.198.43"
DEFAULT_PORT = 8000
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB max file size

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
