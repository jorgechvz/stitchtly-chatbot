from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Environment variables for database connection
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT", "5432")  
database = os.getenv("DB_NAME")

# Validate all required parameters are set
if not all([username, password, host, port, database]):
    raise ValueError("Some required environment variables are missing!")

# Connection string
connection_string = (
    f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
)

# Create the database engine
engine = create_engine(connection_string)

# Function to get a database connection
def get_connection():
    """
    Provides a database connection object.

    Returns:
        connection (Connection): SQLAlchemy connection object.
    """
    connection = engine.connect()
    connection.execute(text("SET search_path TO public"))
    return connection

# Function to test the connection
def test_connection():
    """
    Tests the database connection by executing a simple query.

    Returns:
        version (str): PostgreSQL version information.
    """
    try:
        with get_connection() as connection:
            result = connection.execute(text("SELECT version();"))
            return result.fetchone()
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise
