import os
from dotenv import load_dotenv

load_dotenv()

def get_database_url():
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
    return f"postgresql+psycopg2://{POSTGRES_USER}:?{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"