from database import connect_db, close_database
from scraper import fetch_raw_area_text_from_url
from data_cleaning import parse_area_and_city 
import logging

def fetch_records_with_missing_city(cursor):
    query = "SELECT id, area, url FROM properties_for_rent WHERE city IS NULL OR city = ''"
    cursor.execute(query)
    return cursor.fetchall()

def update_city_for_record(cursor, record_id, area=None, city=None):
    # Build the SET clause based on provided parameters
    set_clauses = []
    params = []

    if area is not None:
        set_clauses.append("area = %s")
        params.append(area)
    if city is not None:
        set_clauses.append("city = %s")
        params.append(city)

    if not set_clauses:
        # If neither area nor city is provided, do nothing
        logging.warning(f"No data to update for record ID {record_id}")
        return

    params.append(record_id)
    set_clause = ", ".join(set_clauses)
    query = f"UPDATE properties_for_rent SET {set_clause} WHERE id = %s"
    cursor.execute(query, params)


def backfill_city_data():
    import logging
    conn = None
    cursor = None
    try:
        # Attempt to connect to the database
        conn, cursor = connect_db()
    except Exception as e:
        # If connection fails, log the error and exit
        logging.error(f"Error connecting to the database: {e}", exc_info=True)
        return

    try:
        # Proceed with fetching records and updating the database
        records = fetch_records_with_missing_city(cursor)
        logging.info(f"Found {len(records)} records with missing city.")

        for record in records:
            record_id, raw_area_text, url = record
            if raw_area_text is None:
                logging.info(f"raw_area_text is None for record ID {record_id}, re-fetching from URL.")
                raw_area_text = fetch_raw_area_text_from_url(url)
                if raw_area_text is None:
                    logging.warning(f"Could not fetch raw_area_text for record ID {record_id}")
                    continue  # Skip this record

            area, city = parse_area_and_city(raw_area_text)
            if city:
                if area:
                    update_city_for_record(cursor, record_id, area=area, city=city)
                    logging.info(f"Updated record ID {record_id}: area set to '{area}', city set to '{city}'")
                else:
                    update_city_for_record(cursor, record_id, city=city)
                    logging.info(f"Updated record ID {record_id}: city set to '{city}'")
            else:
                logging.warning(f"Unable to parse city for record ID {record_id} with area text '{raw_area_text}'")

        conn.commit()
        logging.info("City fields have been updated successfully.")

    except Exception as e:
        # Handle exceptions during record processing
        if conn:
            conn.rollback()
        logging.error(f"An error occurred during backfill: {e}", exc_info=True)
    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    backfill_city_data()
