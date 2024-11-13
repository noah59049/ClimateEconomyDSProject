import numpy as np
import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup
import time
import gc

# Assumptions:

# ChatGPT reads the data from files correctly

#DONE Every dataframe has the same columns

#DONE Every measurement column has the comp_ and meas_ and years_ flag columns associated with it

#DONE comp_flag_ is always 'S' or ' '

# measurement value is -9999 iff comp_flag_ is ' ';

# who knows about the years, there will probably be some assumptions


# ChatGPT wrote this function
def read_csv_from_web(url):
    # Step 1: Fetch the webpage content
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        csv_data = response.text

        # Step 2: Parse the CSV data
        # Use StringIO to read the CSV from the string
        csv_file = StringIO(csv_data)
        df = pd.read_csv(csv_file)

        # Display the DataFrame
        return df
    else:
        print(f"Failed to fetch the webpage. Status code: {response.status_code}")

# And this one
def get_csv_links(url):
    """Fetch all CSV links from the given webpage."""
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    csv_links = []
    for link in soup.find_all('a', href=True):
        if link['href'].endswith('.csv'):
            csv_links.append(link['href'])
    
    # Convert relative URLs to absolute URLs if necessary
    base_url = url.rsplit('/', 1)[0]  # Get the base URL
    csv_links = [link if link.startswith('http') else f"{base_url}/{link}" for link in csv_links]
    
    return csv_links

# I wrote this one though (I think)
def get_ith_dataframe(url, i):
    links = get_csv_links(url)
    df = pd.read_csv(links[i])
    return df


LANDING_PAGE_URL = "https://www.ncei.noaa.gov/data/normals-hourly/1991-2020/access/"
LINKS = get_csv_links(LANDING_PAGE_URL)
NUM_LINKS = 467
assert len(LINKS) == NUM_LINKS

def download_all_data():
    for i, url in enumerate(LINKS):
        df = pd.read_csv(url)
        df.to_csv(f"{i}.csv", index=False)
        del df
        gc.collect()
        print(f"downloaded dataset # {i}")

download_all_data()
print("done")