#---------------------------IMPORTS-------------------------------#

from bs4 import BeautifulSoup
import requests
import pandas as pd
from time import sleep
import re
from tqdm import tqdm
from random import uniform

#-------------------------PREPARATION-----------------------------#

def prepare_url(payment, location):
    """
    Prepares the URL for scraping based on intention of renting/buying 
    and the location of interest

    Args:
        payment (str): Either 'rent' or 'sale'
        location (str): The city or location for scraping properties

    Returns:
        str: The full URL for the search
    """
    return 'https://www.mubawab.ma/en/ct/{}/real-estate-for-{}'.format(
        payment, location
    )

def get_links(url, max_pages=20):
    prop_links = []
    
    page = 1  # Start from the first page
    while page <= max_pages:
        print(f'Scraping links from page {page}...')
        page_url = url + f':p:{page}'
        
        try:
            response = requests.get(page_url)
            response.raise_for_status()  # Raise an error for bad status codes
            
            soup = BeautifulSoup(response.content, 'html.parser')
            listings = soup.find_all('h2', class_='listingTit')

            # If no listings are found, break the loop
            if not listings:
                print(f"No listings found on page {page}. Stopping pagination.")
                break

            for listing in listings:
                try:
                    link_tag = listing.find('a')
                    if link_tag and 'href' in link_tag.attrs:
                        link = link_tag['href']
                        prop_links.append(link)
                    else:
                        print(f"Link not found in listing: {listing}")
                
                except AttributeError as e:
                    print(f"Error finding a link: {e}")

            # Check if there is a "Next" page
            next_page = soup.find('a', class_='arrowDot')
            if not next_page:
                print("No 'Next' page found. Stopping pagination.")
                break
        
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred on page {page}: {http_err}")
            break  # Stop the loop if we hit an HTTP error (like 404)
        
        except requests.RequestException as req_e:
            print(f"Request error on page {page}: {req_e}")
            break  # Stop the loop for other request issues

        print('Sleeping for a bit...')
        sleep(uniform(1, 3))  # Random sleep between 1 and 3 seconds

        # Increment page counter if there is a next page
        page += 1

    return prop_links

#-------------------------FEATURES--------------------------------#

def get_details(links):
    full_list = []
    for counter, link in enumerate(tqdm(links, desc="Fetching property details")):
        try:
            counter += 1
            response = requests.get(link)
            soup = BeautifulSoup(response.content, 'html.parser')

            price = soup.find('h3', class_='orangeTit').text.strip()
            
            area_text = soup.find('h3', class_='greyTit').text.strip()
            pattern = r'^(.*)\sin\s(.*)$'

            # Use re.search to match both the area and the city
            match = re.search(pattern, area_text)

            if match:
                area = match.group(1).strip()  # First group: Area
                city = match.group(2).strip()  # Second group: City
            
            title = soup.find('h1', class_='searchTitle').text.strip()
            
            description = soup.find_all('p', class_='adMainFeatureContentValue')

            description_titles = ['Property Type', 'Condition', 'Age', 'Floor', 'Orientation', 'Floor']
            descriptor_list = [desc.text.strip() for desc in description]

            desc_dict = dict(zip(description_titles, descriptor_list))
            
            size, rooms, bedrooms, bathrooms = None, None, None, None

            details = soup.find_all('div', class_='adDetailFeature')

            for detail in details:
                # Check for size (since it's the first one with 'm²')
                if 'm²' in detail.text:
                    size = detail.find('span').text.strip().replace('m²', '').strip()
                
                # Check for number of pieces
                if 'Pieces' in detail.text:
                    rooms = detail.find('span').text.strip().replace('Pieces', '').strip()
                
                # Check for number of rooms
                if 'Rooms' in detail.text:
                    bedrooms = detail.find('span').text.strip().replace('Rooms', '').strip()
                
                # Check for number of bathrooms
                if 'Bathrooms' in detail.text:
                    bathrooms = detail.find('span').text.strip().replace('Bathrooms', '').strip()
                    
            features = soup.find_all('span', class_='fSize11 centered')
            feature_list = [feature.text.strip() for feature in features]   
            feature_str = ', '.join(feature_list)
                     
            property_details = {
                                'Title': title,
                                'City' : city, 
                                'Area': area, 
                                'Size': size, 
                                'Rooms': rooms, 
                                'Bedrooms': bedrooms, 
                                'Bathrooms': bathrooms, 
                                'Price': price,
                                'Features': feature_str
                                }
            
            all_details = {**property_details, **desc_dict}
            
            full_list.append(all_details)
            
            sleep(uniform(1, 3))
            
        except requests.RequestException as req_e:
            print(f'Error fetching property data: {req_e}')
        except AttributeError as attr_e:
            print(f'Missing element in the page: {attr_e}')
            
    return pd.DataFrame(full_list)