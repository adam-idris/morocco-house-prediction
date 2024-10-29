import os
import psycopg2
import logging
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env file

def initialise_database():
    """
    Intialises the database connection and ensures the necessary tables 
    exist.

    Returns:
        tuple: A tuple containing the database connection and cursor.
    """
    try:
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASS'],
            port=os.environ['DB_PORT']
        )
        cursor = conn.cursor()
        # Create table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS properties (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE,
                title TEXT,
                city TEXT,
                area TEXT,
                size REAL,
                rooms INTEGER,
                bedrooms INTEGER,
                bathrooms INTEGER,
                price TEXT,
                features TEXT,
                property_type TEXT,
                condition TEXT,
                age TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        conn.commit()
        return conn, cursor
    except Exception as e:
        logging.error(f'Error connecting to the database: {e}')
        raise

def is_url_scraped(cursor, url):
    """
    Checks if the URL is already present in the properties table

    Args:
        cursor (psycopg2.extensions.cursor): The database cursor.
        url (str): The URL to check.

    Returns:
        bool: True if the URL exists, False otherwise.
    """
    cursor.execute('SELECT 1 FROM properties WHERE url = %s', (url,))
    return cursor.fetchone() is not None

def save_scraped_url(cursor, conn, url):
    cursor.execute('INSERT INTO properties (url) VALUES (%s) ON CONFLICT \
        DO NOTHING', (url,))
    conn.commit()