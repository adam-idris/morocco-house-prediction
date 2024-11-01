from scraper import prepare_url, get_links, get_details
from database import *
import logging
import pandas as pd

def main():
    cities = [
        "casablanca", "rabat", "dar-bouazza", "mohammédia", "meknès", "bouznika", 
        "oujda", "berrechid", "sidi-rahal", "safi", "harhoura", "tamesna", 
        "al-hoceïma", "sidi-rahal-chatai", "deroua", "sidi-bouknadel", "ait-melloul", 
        "sidi-allal-el-bahraoui", "tiznit", "ain-attig", "sidi-abdallah-ghiat", 
        "ksar-sghir", "sidi-bouzid", "bir-jdid", "marrakech", "agadir", "kénitra", 
        "témara", "el-jadida", "martil", "asilah", "saïdia", "nador", "m'diq", 
        "nouaceur", "béni-mellal", "mehdia", "chefchaouen", "taghazout", 
        "taroudant", "ourika", "oued-laou", "médiouna", "berkane", "tiflet", 
        "ifrane", "khémisset", "taza", "tanger", "bouskoura", "fès", "salé", 
        "essaouira", "tétouan", "el-mansouria", "benslimane", "skhirat", "el-menzeh", 
        "had-soualem", "zenata", "errahma", "settat", "cabo-negro", "larache", 
        "fnideq", "tit-mellil", "ain-aouda", "azemmour", "khouribga", "ben-guerir", 
        "azrou", "ouarzazate"
    ]
    conn = None
    cursor = None
    try:
        # Initialise the database connection once
        conn, cursor = initialise_database()
        for city in cities:
            try:
                url = prepare_url(city, 'rent')
                links = get_links(url, max_pages=2, cursor=cursor)
                if links:
                    properties = get_details(links)
                    if not properties.empty:
                        insert_properties(cursor, properties.to_dict('records'))
                    else:
                        logging.info('No new properties to insert.')
                else:
                    logging.info('No new property links found.')
            except Exception as e:
                logging.error(f'An error occurred: {e}')
        conn.commit()
    except Exception as e:
        logging.error(f'An error occurred during database initialization: {e}', exc_info=True)
    finally:
        if conn and cursor:
            close_database(conn, cursor)

if __name__ == '__main__':
    main()