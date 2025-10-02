""" 
This ingestion server accepts CSI data sent from all of the RPI collectors
in the form of Post requests containing a binary file with a collection of CSI messages recevied
and a json array containing the metadata associated with each CSI message in the binary file
it then places the binary files in an S3 server, and the meta data in a dynamoDB
"""
from datetime import datetime
import logging
from typing import Optional
from fastapi import FastAPI, HTTPException, Form, File
from fastapi.responses import StreamingResponse
import uvicorn
import boto3
from pydantic import BaseModel, ValidationError
from botocore.exceptions import ClientError, NoCredentialsError
from _CONST import (
    AWS_REGION, S3_BUCKET_NAME, DYNAMODB_TABLE_NAME,
    LOG_LEVEL, LOG_FORMAT, DEFAULT_HOST, DEFAULT_PORT
)
from typing import List
import json
import os
import io 
import zipfile

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
    
class CSI_Metadata(BaseModel):
    mac_address : str
    timestamp : float
    device_name : str
    offset_in_file : int
    message_size : int
    message_id : int
    access_point : int
    channel_number : int
    matrix_rows : int
    matrix_columns : int
    bandwidth : int
    spatial_channels : int
    rssi : int
    fc : int
    sequence_number : int 

    
    # if timestamps are given request on partition and sort keys
""" ENDPOINT TO UPLOAD DATA TO S3 OR STORE LOCALLY ON SERVER"""
@app.post("/ingest")
async def ingest_csi_data(
    metadata = Form(...),
    save_to_server: str = Form(...),
    save_to_s3_storage: str = Form(...),
    csi_blob = File(...)
):
    # Convert string boolean values to actual booleans
    save_to_server_bool = save_to_server.lower() in ['true', '1', 'yes']
    save_to_s3_storage_bool = save_to_s3_storage.lower() in ['true', '1', 'yes']
    
    LOG.info(f"Received: save_to_server='{save_to_server}' -> {save_to_server_bool}")
    LOG.info(f"Received: save_to_s3_storage='{save_to_s3_storage}' -> {save_to_s3_storage_bool}")
    
    # 422 error code means "your inputed data was invalid, something is wrong on your end"
    # parse received CSI metadata
    try:
        metadata_list = json.loads(metadata)
    except json.JSONDecodeError as e:
        LOG.error(f"error parsing JSON string {e}")
        raise HTTPException(status_code=422, detail="Invalid JSON metadata")
    if not metadata_list:
        LOG.error("JSON string empty")
        raise HTTPException(status_code=422, detail="No metadata provided")
     
    # validate the metadata matches accepted schema
    # and grab the earliest timestamp and device name so we can properly name the file 
    earliest_timestamp = float('inf')
    device_name = ""
    validated_metadata: List[CSI_Metadata] = []
    for packet in metadata_list:
        try:
            validated_packet = CSI_Metadata(**packet)
            validated_packet.timestamp = int(validated_packet.timestamp * 1000) # save the timestamp to the millisecond
            validated_metadata.append(validated_packet)
            earliest_timestamp = min(earliest_timestamp, validated_packet.timestamp)
            device_name = validated_packet.device_name
        except ValidationError as e:
            LOG.error(f"JSON string invalid with schema {packet}")
            raise HTTPException(status_code=422, detail="Invalid metadata packet")
    (file_name, path_name) = get_file_name_and_location(device_name, earliest_timestamp)
    
    for packet in validated_metadata:
        # update packets with location on s3 server
        validated_metadata.location_on_s3_server = path_name + file_name

    if(save_to_server_bool):
        # if file save was unsuccessful return in error
        if(save_upload_file(path_name, file_name, csi_blob) == False):
            LOG.error(f"unable to save file {file_name} to following location {path_name}")
            raise HTTPException(status_code=500, detail = "Unable to save file localy")
        # TODO: FIGURE OUT WHAT TO DO WITH METADATA LOCALLY 
    if(save_to_s3_storage_bool):
        # ensure file is at head
        csi_blob.file.seek(0)
        # batch write metadata into dynamoDB 
        with dynamodb_table.batch_writer() as batch:
            for validated_packet in validated_metadata:
                batch.put_item(Item=validated_packet.dict())
        # save file into S3 storage 
        # THIS IS A MASSIVE BOTTLENECK, IT WILL TAKE ANYWHERE FROM 1-60 SECS FOR US TO RECEIVE A RESPONSE HERE
        # SHOULD WE FAIL SILENTLY/HANDLE THIS ANY OTHER WAY?
        try:
            s3_client.upload_fileobj(csi_blob.file, S3_BUCKET_NAME, path_name + file_name)
        except Exception as e:
            LOG.error(f"unable to upload file {file_name} to S3 storage {e}")
            raise HTTPException(status_code=500, detail="unable to save file to s3 server")
    
    # return success message
    return {"message": "File ingested successfully", "filename": file_name}

""" helper function given a file saves to local storage and returns whether it was successful """

def save_upload_file(path: str, file_name: str, file) -> bool:
    # if directory doesn't exist create it 
    try:
        os.makedirs(path, exist_ok=True)
        full_path = os.path.join(path, file_name)
        
        # Save the file efficiently in chunks
        with open(full_path, "wb") as buffer:
            while content := file.file.read(1024 * 1024):
                buffer.write(content)
        
        return True
    except Exception as e:
        LOG.error(f"Error saving file: {e}")
        return False

""" helper function returning filename and location of file """ 
def get_file_name_and_location(device_name, timestamp):
    dt = datetime.fromtimestamp(timestamp // 1000) # expected to be in seconds 
    file_name = (
         f"{device_name}_"
        f"{dt.year}_{dt.month:02d}_{dt.day:02d}_"
        f"{dt.hour:02d}_{dt.minute:02d}_{dt.second:02d}"
        f".bin"
    )
    path_name = (
        f"csi-data/"
        f"{device_name}/"
        f"{dt.year}/{dt.month:02d}/{dt.day:02d}_"
        f"{dt.hour:02d}/{dt.minute:02d}/"
    )
    
    return (file_name, path_name)
@app.get("/health")
def health():
    """Health check endpoint to verify server and AWS services status."""
    health_status = { "status": "ok" }
    return health_status


if __name__ == "__main__":
    uvicorn.run("app:app", host=DEFAULT_HOST, port=DEFAULT_PORT, reload=True)