import os
from dotenv import load_dotenv # type: ignore

# loading all the env variables from .env file
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
S3_MODEL_DIR_KEY = os.getenv("S3_MODEL_DIR_KEY")
S3_MODEL_BUCKET_NAME = os.getenv("S3_MODEL_BUCKET_NAME")
MODEL_SAVED_DIR = os.getenv("MODEL_SAVED_DIR")
