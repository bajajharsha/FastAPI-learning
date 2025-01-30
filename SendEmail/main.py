from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, EmailStr
from datetime import datetime
from fastapi.responses import Response
from uuid import uuid4

app = FastAPI()

# MongoDB Configuration
client = AsyncIOMotorClient(
    "mongodb://localhost:27017",
    maxPoolSize=10,
    minPoolSize=2
    )
db = client.logs_db
logs_collection = db.request_logs
email_logs_collection = db.email_logs  # Collection for email task logs

@app.get("/pool-status")
async def pool_status():
    server_info = await client.server_info()
    return {
        "server_info": server_info,
        "pool_size": client.max_pool_size,
        # "min_pool_size": client.min_pool_size,
    }

# Middleware to log requests and responses into MongoDB
@app.middleware("http")
async def log_requests_to_mongodb(request: Request, call_next):
    start_time = datetime.utcnow()

    # Clone the request body
    request_body = await request.body()

    # Call the next middleware or endpoint handler
    response = await call_next(request)

    # Read the response body (if it’s a StreamingResponse)
    response_body = b""
    async for chunk in response.body_iterator:
        response_body += chunk

    # Log request and response details to MongoDB
    log_entry = {
        "method": request.method,
        "url": str(request.url),
        "request_headers": dict(request.headers),
        "request_body": request_body.decode("utf-8"),
        "response_status": response.status_code,
        "response_body": response_body.decode("utf-8"),
        "response_headers": dict(response.headers),
        "timestamp": start_time,
    }
    await logs_collection.insert_one(log_entry)

    # Return the response with the original body
    return Response(
        content=response_body,
        status_code=response.status_code,
        headers=dict(response.headers),
        media_type=response.media_type,
    )


# Email Configuration
conf = ConnectionConfig(
    MAIL_USERNAME="harshabajaj2011@gmail.com",
    MAIL_PASSWORD="",
    MAIL_FROM="harshabajaj2011@gmail.com",
    MAIL_PORT=587,
    MAIL_SERVER="smtp.gmail.com",
    MAIL_STARTTLS=True,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
)

# Email Input Model
class EmailSchema(BaseModel):
    email: EmailStr
    subject: str
    body: str

# Function to send email
async def send_email(email: EmailSchema, task_id: str):
    try:
        message = MessageSchema(
            subject=email.subject,
            recipients=[email.email],  # List of recipients
            body=email.body,
            subtype="plain",
        )

        fast_mail = FastMail(conf)
        await fast_mail.send_message(message)
        print(f"Email sent to {email.email}")

        # Log success
        await email_logs_collection.insert_one({
            "task_id": task_id,
            "email": email.email,
            "status": "success",
            "timestamp": datetime.utcnow(),
        })
    except Exception as e:
        # Log failure
        await email_logs_collection.insert_one({
            "task_id": task_id,
            "email": email.email,
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.utcnow(),
        })
        print(f"Failed to send email to {email.email}: {str(e)}")

# Endpoint to send email using a background task
@app.post("/send-email/")
async def send_email_endpoint(email: EmailSchema, background_tasks: BackgroundTasks):
    task_id = str(uuid4())  # Generate a unique task ID
    background_tasks.add_task(send_email, email, task_id)
    return {"message": "Email has been scheduled to be sent.", "task_id": task_id}
