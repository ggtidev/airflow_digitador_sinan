import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("API_DB_HOST"),
        port=os.getenv("API_DB_PORT"),
        dbname=os.getenv("API_DB_NAME"),
        user=os.getenv("API_DB_USER"),
        password=os.getenv("API_DB_PASSWORD")
    )
