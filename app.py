""" 
This ingestion server accepts CSI data sent from all of the RPI collectors
in the form of Post requests containing a binary file with a collection of CSI messages recevied
and a json array containing the metadata associated with each CSI message in the binary file
it then places the binary files in an S3 server, and the meta data in a dynamoDB
"""
import logging
from typing import Optional
from fastapi import FastAPI, HTTPException
import uvicorn
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from _CONST import (
    AWS_REGION, S3_BUCKET_NAME, DYNAMODB_TABLE_NAME,
    LOG_LEVEL, LOG_FORMAT, DEFAULT_HOST, DEFAULT_PORT
)

app = FastAPI(title="WiROS Ingestion Server")

# Global client variables - will be initialized on startup
s3_client: Optional[boto3.client] = None
dynamodb_resource: Optional[boto3.resource] = None
dynamodb_table = None

LOG = logging.getLogger("ingestion_server")
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)


@app.on_event("startup")
async def startup_event():
    """Initialize AWS clients and validate connections on server startup."""
    global s3_client, dynamodb_resource, dynamodb_table
    
    LOG.info("Starting WiROS Ingestion Server...")
    """
    try:
        # Initialize S3 client
        LOG.info("Initializing S3 client...")
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # Test S3 connection by checking if bucket exists
        try:
            s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            LOG.info(f"Successfully connected to S3 bucket: {S3_BUCKET_NAME}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                LOG.warning(f"S3 bucket {S3_BUCKET_NAME} does not exist. You may need to create it.")
            else:
                LOG.error(f"Error accessing S3 bucket: {e}")
                raise
        
        # Initialize DynamoDB resource
        LOG.info("Initializing DynamoDB resource...")
        dynamodb_resource = boto3.resource('dynamodb', region_name=AWS_REGION)
        dynamodb_table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
        
        # Test DynamoDB connection
        try:
            table_status = dynamodb_table.table_status
            LOG.info(f"Successfully connected to DynamoDB table: {DYNAMODB_TABLE_NAME} (Status: {table_status})")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                LOG.warning(f"DynamoDB table {DYNAMODB_TABLE_NAME} does not exist. You may need to create it.")
            else:
                LOG.error(f"Error accessing DynamoDB table: {e}")
                raise
        
        LOG.info("Server startup completed successfully!")
        
    except NoCredentialsError:
        LOG.error("AWS credentials not found. Please configure your AWS credentials.")
        raise
    except Exception as e:
        LOG.error(f"Failed to initialize AWS clients: {e}")
        raise
    """


@app.post("/ingest")
async def ingest_csi_data():
    """
    Placeholder endpoint for CSI data ingestion.
    
    TODO: This endpoint will accept:
    - Binary file containing CSI messages
    - JSON metadata array for each CSI message
    
    The endpoint will:
    1. Validate the incoming data format
    2. Store binary file in S3
    3. Store metadata in DynamoDB
    4. Return confirmation with storage locations
    """
    
    """
    # Validate that clients are initialized
    if s3_client is None or dynamodb_table is None:
        raise HTTPException(
            status_code=503, 
            detail="Server not properly initialized. AWS clients not available."
        )
    
    # TODO: Implement actual data processing logic
    LOG.info("CSI data ingestion endpoint called (placeholder)")
    
    """

@app.get("/health")
def health():
    """Health check endpoint to verify server and AWS services status."""
    health_status = { "status": "ok" }
    return health_status


if __name__ == "__main__":
    uvicorn.run("app:app", host=DEFAULT_HOST, port=DEFAULT_PORT, reload=True)