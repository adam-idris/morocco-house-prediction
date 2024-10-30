from data_cleaning import *
from datetime import datetime as dt
import datetime
from database import is_url_scraped
from bs4 import BeautifulSoup
import requests
import pandas as pd
from time import sleep
import re
from tqdm import tqdm
from random import uniform
import logging

def prepare_url(location, payment):
    """
    Prepares the URL for scraping based on intention of renting/buying 
    and the location of interest.

    Args:
        location (str): The city or location for scraping properties.
        payment (str): Either 'rent' or 'sale.'

    Returns:
        str: The full URL of the page containing all the listings.
    """
    return 'https://www.mubawab.ma/en/ct/{}/real-estate-for-{}:o:n'.format(
        location, payment
    )

def extract_publication_date(detail):
    try:
        span = detail.find('span', class_='listingDetails iconPadR')
        if span:
            i_tag = span.find('i')
            if i_tag and i_tag.next_sibling:
                published_text = i_tag.next_sibling.strip('Published ').strip()
                
                if published_text == 'today':
                    publication_date = dt.today()
                else:
                    days_ago = clean_integer(published_text)
                    publication_date = (dt.today() - datetime.timedelta(days=days_ago))
                    
                return publication_date
        
        else:
            publication_date = None
        return None
    
    except Exception as e:
        logging.error(f'Error extracting publication date: {e}')
        return None

def get_links(url, max_pages=20, cursor=None):
    """
    Scrapes property links from mubaweb.ma and handles pagination.

    Args:
        url (str): The base URL containing all the property listings.
        max_pages (int, optional): Number of pages to scrape. Defaults to 20.

    Returns:
        list: URLs of all the specific property pages to be scraped.
    """
    prop_links = []
    page = 1  # Start from the first page
    while page <= max_pages:
        page_url = url + f':p:{page}'
        
        try:
            response = requests.get(page_url)
            response.raise_for_status()  # Raise an error for bad status codes
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all the listing links on the page
            listings = soup.find_all('li', class_='listingBox')

            # If no listings are found, break the loop
            if not listings:
                logging.info(f"No listings found on page {page}. Stopping pagination.")
                break

            for listing in listings:
                try:
                    link_tag = listing.find('h2', class_='listingTit').find('a')
                    if link_tag and 'href' in link_tag.attrs:
                        link = link_tag['href']
                    else:
                        continue
                    
                    if cursor and is_url_scraped(cursor, link):
                        continue
                    
                    detail = listing.find('div', class_='controlBar sMargTop')
                    publication_date = extract_publication_date(detail)

                    # Append to list
                    prop_links.append((link, publication_date))
                
                except AttributeError as e:
                    logging.error(f"Error finding a link: {e}")

            # Check if there is a "Next" page
            next_page = soup.find('a', class_='arrowDot')
            if not next_page:
                logging.info("No 'Next' page found. Stopping pagination.")
                break
            
            page += 1
            # Random sleep between 1 and 3 seconds to prevent overloading the server
            sleep(uniform(1, 3))
            
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred on page {page}: {http_err}")
            break  # Stop the loop if we hit an HTTP error
        
        except requests.RequestException as req_e:
            logging.error(f"Request error on page {page}: {req_e}")
            break  # Stop the loop for other request issues

    return prop_links

def get_details(links_with_dates):
    """
    Scrapes the important features of each property.

    Args:
        links (str): The URLs of each property to be scraped.
        cursor (psycopg2.extensions.cursor): The database cursor.

    Returns:
        DataFrame: A pandas DataFrame containing all the features of the 
        property
    """
    full_list = []
    
    for link, publication_date in tqdm(links_with_dates, desc="Fetching property details"):
        try:
            response = requests.get(link)
            soup = BeautifulSoup(response.content, 'html.parser')

            raw_price = soup.find('h3', class_='orangeTit').text.strip()
            raw_area_text = soup.find('h3', class_='greyTit').text.strip()
            raw_title = soup.find('h1', class_='searchTitle').text.strip()
            
            price = clean_integer(raw_price)
            title = clean_text(raw_title)
            
            area, city = parse_area_and_city(raw_area_text)
            
            text_content = None
            div_block = soup.find('div', class_='blockProp')
            if div_block:
                p_tag = div_block.find('p')
                if p_tag:
                    text_content = p_tag.get_text(separator=" ").strip()
            
            description = soup.find_all(
                'p', class_='adMainFeatureContentValue')
            
            description_titles = ['Property Type', 'Condition', 'Age']
            descriptor_list = [clean_text(desc.text) for desc in description]
            desc_dict = dict(zip(description_titles, descriptor_list))
            
            size = rooms = bedrooms = bathrooms = None
            # This is extra info like size, number of rooms etc.
            details = soup.find_all('div', class_='adDetailFeature')

            for detail in details:
                text = detail.text
                value = detail.find('span').text.strip()
                
                # Check for size (since it's the first one with 'm²')
                if 'm²' in text:
                    size = clean_integer(value)
                
                # Check for number of rooms (called pieces on site)
                if 'Pieces' in text or 'Piece' in text:
                    rooms = clean_integer(value)
                
                # Check for number of bedrooms
                if 'Rooms' in text or 'Room' in text:
                    bedrooms = clean_integer(value)
                
                # Check for number of bathrooms
                if 'Bathrooms' in text or 'Bathroom' in text:
                    bathrooms = clean_integer(value)
                    
            # If there are no rooms extracted, the function searches the description
            if rooms is None and text_content:
                rooms = clean_rooms(text_content)
                    
            features = soup.find_all('span', class_='fSize11 centered')
            feature_list = [clean_text(feature.text) for feature in features]   
            feature_str = ', '.join(filter(None, feature_list))
                     
            property_details = {
                                'title': title,
                                'description': text_content,
                                'property_type': desc_dict.get('Property Type'),
                                'city' : city, 
                                'area': area, 
                                'size': size, 
                                'rooms': rooms, 
                                'bedrooms': bedrooms, 
                                'bathrooms': bathrooms, 
                                'price': price,
                                'features': feature_str,
                                'condition': clean_condition(desc_dict.get('Condition')),
                                'age': clean_age(desc_dict.get('Age')),
                                'date_published': publication_date,
                                'url': link
                                }
            
            full_list.append(property_details)
            sleep(uniform(1, 3))
            
        except Exception as e:
            logging.error(f'Error fetching property data from {link}: {e}')
            
    return pd.DataFrame(full_list)

def fetch_raw_area_text_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        soup = BeautifulSoup(response.content, 'html.parser')
        raw_area_text_element = soup.find('h3', class_='greyTit')
        if raw_area_text_element:
            return raw_area_text_element.text.strip()
        else:
            return None
    except Exception as e:
        logging.error(f"Error fetching raw_area_text from URL {url}: {e}")
        return None