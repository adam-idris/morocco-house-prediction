import re
import pandas as pd

def clean_integer(number_str):
    """
    Cleans all numerate fields by removing any non-digit characters, and 
    converting it into an integer value.

    Args:
        number_str (str): The price string of the property.

    Returns:
        int or None: The cleaned price as an integer, or None if invalid.
    """
    
    try:
        if not number_str:
            return None
        number_clean = re.sub(r'[^0-9]', '', number_str)
        if number_clean == '':
            return None
        return int(number_clean)
    except ValueError:
        return None
    
def clean_text(text):
    
    return text.strip() if text else None

def clean_age(age_str):
    """
    Cleans the age string by extracting age ranges in the format 'min-max' 
    only if 'years' is in the original string and exactly two numbers are 
    present.

    Args:
        age_str (str): The age string to clean.

    Returns:
        str or None: The age range in 'min-max' format, or None if conditions 
        are not met.
    """
    if not age_str:
        return None

    # Convert the string to lowercase for case-insensitive matching
    age_str_lower = age_str.lower()

    # Check if 'years' is in the original string
    if 'years' not in age_str_lower:
        return None

    # Extract all numbers from the string
    numbers = re.findall(r'\d+', age_str)
    if len(numbers) == 2:
        # Exactly two numbers found, format as 'min-max'
        min_age = int(numbers[0])
        max_age = int(numbers[1])
        return f"{min_age}-{max_age}"
    else:
        # Either less than or more than two numbers found
        return None
        
def clean_rooms(description):
    pattern = r'(\d+)\s*(?:\w+\s)?rooms?\b'
    match = re.search(pattern, description, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None

def clean_condition(cond_str):
    try:
        if not cond_str:
            return None
        
        if cond_str == 'Good condition':
            cond_str = 'Good'
            return cond_str
        elif cond_str == 'Due for reform':
            cond_str == 'Old'
            return cond_str
        elif cond_str == 'New':
            return cond_str
        else:
            return None
    except ValueError:
        return None

def drop_no_price(df):
    """
    Drops rows from the DataFrame where the 'price' column has missing values
    and converts the column to integer.

    Parameters:
        df (pandas.DataFrame): The DataFrame to process.

    Returns:
        pandas.DataFrame: The DataFrame without rows missing 'price'.
    """
    
    initial_count = df.shape[0]
    df.dropna(subset='price', inplace=True)
    df['price'] = df['price'].astype('int')
    final_count = df.shape[0]
    print(f"Dropped {initial_count - final_count} rows without a price.")
    
    return df