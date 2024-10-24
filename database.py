import os
import psycopg2
import logging
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env file

def initialise_database():
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
                url TEXT PRIMARY KEY
            )
        ''')
        conn.commit()
        return conn, cursor
    except Exception as e:
        logging.error(f'Error connecting to the database: {e}')
        raise

def is_url_scraped(cursor, url):
    cursor.execute('SELECT 1 FROM properties WHERE url = %s', (url,))
    return cursor.fetchone() is not None

def save_scraped_url(cursor, conn, url):
    cursor.execute('INSERT INTO properties (url) VALUES (%s) ON CONFLICT \
        DO NOTHING', (url,))
    conn.commit()