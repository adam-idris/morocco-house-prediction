import os
import psycopg2
import logging
import psycopg2.extras
from dotenv import load_dotenv
import datetime
from data_cleaning import clean_property_data

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
            CREATE TABLE IF NOT EXISTS properties_for_rent (
                id SERIAL PRIMARY KEY,
                title TEXT,
                description TEXT,
                property_type TEXT,
                city TEXT,
                area TEXT,
                size INTEGER,
                rooms INTEGER,
                bedrooms INTEGER,
                bathrooms INTEGER,
                price INTEGER,
                features TEXT,
                condition TEXT,
                age TEXT,
                date_published DATE,
                url TEXT UNIQUE,
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
    try:
        cursor.execute('SELECT 1 FROM properties_for_rent WHERE url = %s', (url,))
        result = cursor.fetchone()
        return result is not None
    except Exception as e:
        logging.error(f'Error checking if URL is scraped: {e}')
        return False
        
def insert_properties(cursor, properties):
    """
    Inserts a list of property dictionaries into the database.

    Args:
        cursor (psycopg2.extensions.cursor): The database cursor.
        properties (list): A list of dictionaries containing property data.
    """
    records = []
    for prop in properties:
        try:
            prop = clean_property_data(prop)
            date_published = prop.get('date_published')
            if date_published and isinstance(date_published, str):
                # Convert date string to datetime object
                date_published = datetime.strptime(date_published, '%Y-%m-%d')
            prop['date_published'] = date_published
            record = (
                prop['title'],
                prop['description'],
                prop['property_type'],
                prop['city'],
                prop['area'],
                prop['size'],
                prop['rooms'],
                prop['bedrooms'],
                prop['bathrooms'],
                prop['price'],
                prop['features'],
                prop['condition'],
                prop['age'],
                prop['date_published'],
                prop['url'],
            )
            records.append(record)
        except Exception as e:
            logging.error(f'Error preparing record for URL {prop["url"]}: {e}')
            continue
        
    if records:
        try:
            insert_query = '''
                INSERT INTO properties_for_rent (
                    title, description, property_type, city, area, size, rooms, 
                    bedrooms, bathrooms, price, features, condition, age, 
                    date_published, url
                ) VALUES %s
                ON CONFLICT (url) DO NOTHING
            '''
            psycopg2.extras.execute_values(
                cursor, insert_query, records, template=None, page_size=100
            )
            cursor.connection.commit()
            logging.info(
                f'Inserted {len(records)} new properties into the database.')
        except Exception as e:
            logging.error(f'Error inserting properties into database: {e}')
            logging.error(f'Problematic record: {records}')
            cursor.connection.rollback()
            
def close_database(conn, cursor):
    """
    Closes the database cursor and connection.

    Args:
        conn (psycopg2.extensions.connection): The database connection.
        cursor (psycopg2.extensions.cursor): The database cursor.
    """
    try:
        cursor.close()
        conn.close()
        logging.info('Database connection closed.')
    except Exception as e:  
        logging.error(f'Error closing database connection: {e}')
        
def connect_db():
    try:
        conn = psycopg2.connect(
                host=os.environ['DB_HOST'],
                database=os.environ['DB_NAME'],
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASS'],
                port=os.environ['DB_PORT']
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return conn, cursor
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise
