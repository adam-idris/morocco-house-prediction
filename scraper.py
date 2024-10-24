from data_cleaning import clean_price, clean_size, clean_integer, clean_text
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
        payment (str): Either 'rent' or 'sale.'
        location (str): The city or location for scraping properties.

    Returns:
        str: The full URL of the page containing all the listings.
    """
    return 'https://www.mubawab.ma/en/ct/{}/real-estate-for-{}'.format(
        payment, location
    )

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
            listings = soup.find_all('h2', class_='listingTit')

            # If no listings are found, break the loop
            if not listings:
                print(f"No listings found on page {page}. 
                      Stopping pagination.")
                break

            for listing in listings:
                try:
                    link_tag = listing.find('a')
                    if link_tag and 'href' in link_tag.attrs:
                        link = link_tag['href']
                        if cursor and is_url_scraped(cursor, link):
                            continue  # Skip already scraped links
                        prop_links.append(link)
                
                except AttributeError as e:
                    print(f"Error finding a link: {e}")

            # Check if there is a "Next" page
            next_page = soup.find('a', class_='arrowDot')
            if not next_page:
                print("No 'Next' page found. Stopping pagination.")
                break
        
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred on page {page}: {http_err}")
            break  # Stop the loop if we hit an HTTP error
        
        except requests.RequestException as req_e:
            print(f"Request error on page {page}: {req_e}")
            break  # Stop the loop for other request issues
        
        # Random sleep between 1 and 3 seconds to prevent overloading the server
        sleep(uniform(1, 3))

        # Increment page counter if there is a next page
        page += 1

    return prop_links

def get_details(links, cursor):
    """
    Scrapes the important features of each property.

    Args:
        links (str): The URLs of each property to be scraped.

    Returns:
        DataFrame: A pandas DataFrame containing all the features of the 
        property
    """
    full_list = []
    
    for link in tqdm(links, desc="Fetching property details"):
        if is_url_scraped(cursor, link):
            continue
        try:
            response = requests.get(link)
            soup = BeautifulSoup(response.content, 'html.parser')

            raw_price = soup.find('h3', class_='orangeTit').text.strip()
            raw_area_text = soup.find('h3', class_='greyTit').text.strip()
            raw_title = soup.find('h1', class_='searchTitle').text.strip()
            
            price = clean_price(raw_price)
            title = clean_text(raw_title)
            
            pattern = r'^(.*)\sin\s(.*)$'

            # Use re.search to match both the area and the city
            match = re.search(pattern, raw_area_text)

            if match:
                area = match.group(1).strip()  # First group: Area
                city = match.group(2).strip()  # Second group: City
            else:
                area = city = None
            
            div_block = soup.find('div', class_='blockProp')
            if div_block:
                p_tag = div_block.find('p')
                if p_tag:
                    text_content = p_tag.get_text(separator=" ").strip()
            
            description = soup.find_all(
                'p', class_='adMainFeatureContentValue')
            
            description_titles = ['Property Type', 'Condition', 'Age', 
                                  'Floor', 'Orientation', 'Floor']
            descriptor_list = [clean_text(desc.text) for desc in description]
            desc_dict = dict(zip(description_titles, descriptor_list))
            
            size = rooms = bedrooms = bathrooms = None
            # This is extra info like size, number of rooms etc.
            details = soup.find_all('div', class_='adDetailFeature')

            for detail in details:
                text = detail.text.strip()
                value = detail.find('span').text.strip()
                
                # Check for size (since it's the first one with 'm²')
                if 'm²' in text:
                    size = clean_size(value)
                
                # Check for number of rooms (called pieces on site)
                if 'Pieces' in text:
                    rooms = clean_integer(value)
                
                # Check for number of bedrooms
                if 'Rooms' in text:
                    bedrooms = clean_integer(value)
                
                # Check for number of bathrooms
                if 'Bathrooms' in text:
                    bathrooms = clean_integer(value)
                    
            features = soup.find_all('span', class_='fSize11 centered')
            feature_list = [clean_text(feature.text) for feature in features]   
            feature_str = ', '.join(filter(None, feature_list))
                     
            property_details = {
                                'title': title,
                                'description': text_content,
                                'city' : city, 
                                'area': area, 
                                'size': size, 
                                'rooms': rooms, 
                                'bedrooms': bedrooms, 
                                'bathrooms': bathrooms, 
                                'price': price,
                                'features': feature_str,
                                'property_type': desc_dict.get('Property Type'),
                                'condition': desc_dict.get('Condition'),
                                'age': desc_dict.get('Age'),
                                'url': link
                                }
            
            full_list.append(property_details)
            sleep(uniform(1, 3))
            
        except Exception as e:
            logging.error(f'Error fetching property data from {link}: {e}')
            
    return pd.DataFrame(full_list)