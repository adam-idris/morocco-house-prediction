import logging
import pandas as pd
from bs4 import BeautifulSoup
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.extractors import (
    extract_publication_date, extract_price, extract_title, 
    extract_area, extract_description, extract_descriptors, 
    extract_details, extract_features
)

from modules.data_cleaning import *

# Sample HTML content simulating property listings
sample_html_content = """
<div class="property-listing">
    <h1 class="searchTitle">Beautiful Apartment in Downtown</h1>
    <h3 class="orangeTit">1,200,000 MAD</h3>
    <h3 class="greyTit">Downtown in Casablanca</h3>
    <div class="blockProp">
        <p>Stunning apartment with a view of the city center.</p>
    </div>
    <div class="adDetailFeature">Size: <span>150 m²</span></div>
    <div class="adDetailFeature">Pieces: <span>3</span></div>
    <div class="adDetailFeature">Rooms: <span>2</span></div>
    <div class="adDetailFeature">Bathrooms: <span>2</span></div>
    <div class="controlBar sMargTop">
        <span class="listingDetails iconPadR"><i></i>Published 3 days ago</span>
    </div>
    <p class="adMainFeatureContentValue">Villa</p>
    <p class="adMainFeatureContentValue">Good condition</p>
    <div class="features">
        <span class="fSize11 centered">Balcony</span>
        <span class="fSize11 centered">Garage</span>
        <span class="fSize11 centered">Elevator</span>
    </div>
</div>
"""

def run_extraction_tests():
    # Parse the sample HTML content with BeautifulSoup
    soup = BeautifulSoup(sample_html_content, 'html.parser')
    detail = soup.find('div', class_='controlBar sMargTop')

    # Extract individual fields using the extraction functions
    title = extract_title(soup)
    price = extract_price(soup)
    area, city = extract_area(soup)
    description = extract_description(soup)
    size, rooms, bedrooms, bathrooms = extract_details(soup)
    publication_date = extract_publication_date(detail)
    descriptors = extract_descriptors(soup)
    features = extract_features(soup)

    # Organize the extracted data into a dictionary
    property_data = {
        'title': title,
        'price': price,
        'city': city,
        'area': area,
        'description': description,
        'size': size,
        'rooms': rooms,
        'bedrooms': bedrooms,
        'bathrooms': bathrooms,
        'publication_date': publication_date,
        'property_type': descriptors.get('Property Type'),
        'condition': clean_condition(descriptors.get('Condition')),
        'age': descriptors.get('Age'),
        'features': features,
        'url': 'http://sample-property-url.com'  # Placeholder URL for testing
    }

    # Display results as a DataFrame
    df = pd.DataFrame([property_data])
    df.to_pickle("test.pkl")
    print("Extracted Property Data as DataFrame:")
    print(df)

if __name__ == "__main__":
    # Set up logging for observing potential issues during testing
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run_extraction_tests()
