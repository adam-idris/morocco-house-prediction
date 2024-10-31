from scraper import prepare_url, get_links, get_details
from database import *
import logging
import pandas as pd

CITIES = [
"casablanca"
]

def process_city(city, cursor):
    """Process property listings for a given city."""
    try:
        url = prepare_url(city, 'rent')
        links = get_links(url, max_pages=1, cursor=cursor)
        if not links:
            logging.info(f'No new property links found for {city}.')
            return

        properties = get_details(links, city)
        if properties.empty:
            logging.info(f'No new properties to insert for {city}.')
        else:
            insert_properties(cursor, properties.to_dict('records'))
    except Exception as e:
        logging.error(f'Error processing {city}: {e}', exc_info=True)

def main():
    """Main function to process property data for each city."""
    conn, cursor = None, None
    try:
        conn, cursor = initialise_database()
        for city in CITIES:
            process_city(city, cursor)
        conn.commit()
    except Exception as e:
        logging.error(f'Database initialization error: {e}', exc_info=True)
    finally:
        if conn and cursor:
            close_database(conn, cursor)

if __name__ == '__main__':
    main()