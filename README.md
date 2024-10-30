# Morocco Real Estate Scraper and Prediction

An end-to-end data pipeline project aimed at scraping real estate listings from a Moroccan property website, cleaning and storing the data in a PostgreSQL database hosted on AWS, and performing exploratory data analysis (EDA) to predict house prices.

## Project Overview

This project focuses on building a data pipeline to:

- Scrape property listings from Mubawab.ma
- Clean and preprocess the scraped data
- Store the data in a PostgreSQL database hosted on AWS
- Perform exploratory data analysis (EDA) for house price prediction

The goal is to collect up-to-date real estate data in Morocco to analyse market trends and build predictive models for house prices.

## Features

- Web Scraping: Extracts detailed property information, including title, description, property type, location, size, price, features, and more.
- *Data Cleaning:* Handles inconsistencies, missing values, and data type conversions.
- *Database Storage:* Stores the cleaned data in a PostgreSQL database hosted on AWS RDS.
- *Data Backfill:* Includes scripts to backfill missing data fields in the database.
- *Modular Design:* Organised codebase with separate modules for scraping, data cleaning, database operations, and utilities.
- *Logging and Error Handling:* Comprehensive logging for debugging and error resolution.
