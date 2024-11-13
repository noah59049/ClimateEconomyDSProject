import csv
import requests
import json

def fetch_series_data():
    headers = {'Content-type': 'application/json'}
    data = json.dumps({
        "seriesid": ['SMS25000009093000001'],
        "startyear": "2010",
        "endyear": "2020"
    })
    
    response = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
    
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print("Error fetching data from API:", response.status_code)
        return None

def parse_series_data(api_data):
    extracted_data = []

    for series in api_data['Results']['series']:
        series_id = series['seriesID']
        
        for item in series['data']:
            # Extract year, period, and value
            year = item['year']
            period = item['period']
            value = item['value']
            
            # Create a label for each entry (customize this as needed)
            label = f"{series_id} ({year}-{period})"
            
            # Add a dictionary with only the selected fields
            extracted_data.append({
                "year": year,
                "period": period,
                "label": label,
                "value": value
            })
    
    return extracted_data

def save_to_csv(parsed_data, filename="filtered_series_data.csv"):
    # Define only the selected fields
    fieldnames = ["year", "period", "label", "value"]
    with open(filename, mode='w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in parsed_data:
            writer.writerow(row)
    print(f"Data has been saved to {filename}")

# Main execution flow
api_data = fetch_series_data()  # Fetch series data from API

if api_data:
    parsed_data = parse_series_data(api_data)  # Parse relevant fields
    save_to_csv(parsed_data)  # Save selected fields to CSV
