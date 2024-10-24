import re

def clean_price(price_str):
    """
    Cleans the price string by removing the currency symbol (DH) and any
    non-digit characters, and converting it into a string.

    Args:
        price_str (str): The price string of the property.

    Returns:
        int or None: The cleaned price as an integer, or None if invalid.
    """
    
    try:
        if not price_str:
            return None
        price_clean = re.sub(r'[^0-9]', '', price_str)
        if price_str == '':
            return None
        return int(price_clean)
    except (ValueError, TypeError):
        return None