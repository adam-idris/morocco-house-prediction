from scraper import prepare_url, get_links, get_details
from database import initialise_database, is_url_scraped, save_scraped_url
from utils import send_error_email
from config import SCRAPER_SETTINGS
import logging

def main():
    try:
        conn, cursor = initialise_database()
        url = prepare_url('rent', 'casablanca')
        new_links = get_links(url, max_pages=1, cursor=cursor)
        if new_links:
            df = get_details(new_links)
            # You can choose to store the scraped data in your AWS RDS database
            # For now, let's save it as a CSV file
            df.to_csv('casablanca.csv', mode='a', header=False, index=False)
            # Save new URLs to the database to avoid duplicates
            for link in new_links:
                save_scraped_url(cursor, conn, link)
            logging.info(
                f'Successfully scraped {len(new_links)} new properties.')
        else:
            logging.info("No new properties found.")
        conn.close()
    except Exception as e:
        logging.error(f'An error occurred: {e}')
        # Optionally, handle the exception or notify yourself

if __name__ == '__main__':
    main()