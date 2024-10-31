import logging
import pandas as pd
import re
from datetime import datetime as dt, timedelta
from random import uniform
from time import sleep
from tqdm import tqdm
from bs4 import BeautifulSoup
import requests
from data_cleaning import *
from database import is_url_scraped


# --- URL and Page Helpers ---
def prepare_url(city, payment):
    """Creates URL for the property listings page based on city and payment"""
    return f'https://www.mubawab.ma/en/ct/{city}/real-estate-for-{payment}:o:n'


def fetch_raw_area_text_from_url(url, city):
    """Extracts area and city text from a fallback URL in case of failure."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        raw_area_text_element = soup.find('h3', class_='greyTit')
        return raw_area_text_element.text.strip() if raw_area_text_element else None
    except Exception as e:
        logging.error(
            f"Error fetching raw_area_text from {city} URL {url}: {e}")
        return None


# --- Data Extraction Helpers ---
def extract_publication_date(detail):
    """Extracts and returns the publication date from listing detail."""
    try:
        span = detail.find('span', class_='listingDetails iconPadR')
        if span:
            published_text = span.find('i').next_sibling.strip(
                'Published ').strip()
            if published_text == 'today':
                return dt.today()
            days_ago = clean_integer(published_text)
            return dt.today() - datetime.timedelta(days=days_ago)
        return None
    except Exception as e:
        logging.error(f'Error extracting publication date: {e}')
        return None


def get_links(url, city, max_pages=20, cursor=None):
    """Scrapes property links from multiple pages on the site."""
    prop_links = []
    for page in range(1, max_pages + 1):
        try:
            page_url = url + f':p:{page}'
            response = requests.get(page_url)
            response.raise_for_status()  # Raise an error for bad status codes
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all the listing links on the page
            listings = soup.find_all('li', class_='listingBox')
            # If no listings are found, break the loop
            if not listings:
                logging.info(
                    f"No listings found on page {page}. Stopping pagination."
                )
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
                    publication_date = extract_publication_date(
                        listing.find('div', class_='controlBar sMargTop')
                    )
                    prop_links.append((link, publication_date))
                sleep(uniform(1, 3))
                
        except requests.RequestException as e:
            logging.error(f"Request error for {city} on page {page}: {e}")
            break
    return prop_links


def get_details(links_with_dates, city):
    """Scrapes and returns property details from each property link."""
    full_list = []
    for link, publication_date in tqdm(
        links_with_dates, desc=f"Fetching property details from {city}"):
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
                    
            # function searches the description if no rooms were extracted
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
                                'condition': clean_condition(
                                    desc_dict.get('Condition')),
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
    """
    Extracts the text of the area and city directly from the url. It is a 
    fallback option, if the text isn't extracted properly initially.

    Args:
        url (str): url of the listing the area text needs to be extracted 
        from.

    Returns:
        raw_area_text (str): The text containing area and city information.
    """
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