from contextlib import asynccontextmanager
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, Request, Response
import os
import asyncpg
from datetime import datetime
from logger import logger
from google.cloud import storage
from dotenv import load_dotenv
import uuid
import json
from motor.motor_asyncio import AsyncIOMotorClient

# Load environment variables from .env file
load_dotenv()

# Utils

# Initialize MongoDB Client
async def get_mongo_client():
    mongo_uri = os.getenv("MONGO_URI")
    return AsyncIOMotorClient(mongo_uri)

async def get_db_pool():
    # collection of reusable database connections
    pool = await asyncpg.create_pool(
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )
    return pool

async def process_file(cloud_url: str):
    # Process the file in the background
    print(f"Processing file from {cloud_url}")

# Background Task to Upload File to Google Cloud Storage
async def upload_to_gcs(file_content: bytes, filename: str, content_type: str):
    logger.info(f"Starting background upload for {filename}")

    pool = app.state.pool
    cloud_url = f"https://storage.googleapis.com/{bucket_name}/{filename}"

    async with pool.acquire() as conn:
        # Insert the initial record with 'uploading' status
        logger.info(f"Inserting record for {filename} into database")
        await conn.execute("""
            INSERT INTO uploads (filename, timestamp, file_size, cloud_url, status) 
            VALUES ($1, $2, $3, $4, $5)
        """, filename, datetime.now(), len(file_content), cloud_url, "uploading")

    try:
        # Upload the file to Google Cloud
        blob = bucket.blob(filename)
        blob.upload_from_string(file_content, content_type=content_type)

        # Update status to 'success' after upload completes
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE uploads SET status = 'success' WHERE filename = $1
            """, filename)

        logger.info(f"File {filename} uploaded successfully to {cloud_url}")

    except Exception as e:
        logger.error(f"Upload failed for {filename}: {str(e)}")

        # Update status to 'failed' if an error occurs
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE uploads SET status = 'failed' WHERE filename = $1
            """, filename)



# Lifespan context manager for app startup and shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create the database connection pool on startup
    pool = await get_db_pool()
    client = await get_mongo_client()
    
    # Create table if not exists
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS uploads (
                id SERIAL PRIMARY KEY,
                filename TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                file_size BIGINT NOT NULL,
                cloud_url TEXT NOT NULL,
                status TEXT DEFAULT 'uploading'
            )
        """)

    # store the pool in the app state
    app.state.pool = pool
    app.state.client = client
    
    # Yield control to FastAPI
    yield
    
    # Close the database pool on shutdown
    await pool.close()

app = FastAPI(lifespan=lifespan)


# Middleware for Logging Requests & Responses in MongoDB
@app.middleware("http")
async def log_request_response(request: Request, call_next):
    """Middleware to log request and response in MongoDB"""
    request_id = str(uuid.uuid4())  # Unique ID for tracking
    request_info = {
        "id": request_id,
        "timestamp": datetime.now(),
        "method": request.method,
        "url": str(request.url),
        "headers": dict(request.headers),
        "query_params": dict(request.query_params),
        "client_host": request.client.host if request.client else None,
        "filename": None,  # To be filled if it's an upload request
    }

    try:
        body = await request.body()
        request_info["body"] = json.loads(body.decode("utf-8")) if body else None
    except Exception:
        request_info["body"] = "Non-JSON body"

    response = await call_next(request)
    
    response_body = b""
    async for chunk in response.body_iterator:
        response_body += chunk
    response_info = {
        "status_code": response.status_code,
        "headers": dict(response.headers),
        "body": json.loads(response_body.decode("utf-8")) if response_body else None,
    }

    # Insert into MongoDB
    mongo_client = app.state.client
    db = mongo_client.get_database("upload_file_logs")  # Change DB name as needed
    collection = db.get_collection("upload_file_logs")

    log_entry = {
        "request": request_info,
        "response": response_info,
    }
    await collection.insert_one(log_entry)

    return Response(content=response_body, status_code=response.status_code, headers=dict(response.headers))


'''
1. Upload File to Google Cloud

blob: refers to an object stored in the storage bucket
upload_from_file: is used to upload a file to Google Cloud Storage directly from a file-like object, such as a stream or an in-memory file.

'''

# Initialize Google Cloud Storage Client
storage_client = storage.Client.from_service_account_json(os.getenv("GOOGLE_CLOUD_CREDENTIALS_PATH"))
logger.info("Google Cloud Storage client initialized")
bucket_name = os.getenv("GCS_BUCKET_NAME")
bucket = storage_client.bucket(bucket_name)

@app.post("/upload/")
async def upload_file(file: UploadFile, background_tasks: BackgroundTasks):
    # Replace spaces in filename with underscores
    filename = file.filename.replace(" ", "_")
    
    logger.info(f"Uploading file {filename} to Google Cloud Storage")
    
    # Read file content as bytes
    file_content = await file.read()
    
    # upload process as background task
    background_tasks.add_task(upload_to_gcs, file_content, filename, file.content_type)

    return {"message": "File is being uploaded...", "filename": filename}
    
# Polling Endpoint to Check Upload Status**
@app.get("/status/{filename}")
async def get_upload_status(filename: str):
    filename = filename.replace(" ", "_")
    pool = app.state.pool
    
    async with pool.acquire() as conn:
        result = await conn.fetchrow("SELECT status, cloud_url FROM uploads WHERE filename = $1", filename)
        
    if not result:
        return {"error": "File not found or error uploading file"}

    return {"filename": filename, "status": result["status"], "cloud_url": result["cloud_url"] if result["status"] == "success" else None}
        
