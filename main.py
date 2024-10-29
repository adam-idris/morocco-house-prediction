from scraper import prepare_url, get_links, get_details
import logging
import pandas as pd

def main():
    try:
        url = prepare_url('casablanca', 'rent')
        links = get_links(url, max_pages=1)
        if links:
            properties = get_details(links[:5])
            df = pd.DataFrame(properties)
            df.to_csv('properties.csv')
        else:
            logging.info("No new properties found.")
    except Exception as e:
        logging.error(f'An error occurred: {e}')

if __name__ == '__main__':
    main()