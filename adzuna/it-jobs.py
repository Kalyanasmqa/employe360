import requests
from pymongo import MongoClient

# MongoDB connection settings
mongo_uri = "mongodb://localhost:27017/"
database_name = "adzuna"

# Adzuna API settings
adzuna_api_base_url = "https://api.adzuna.com/v1/api/jobs/gb/search"
adzuna_app_id = "25ad7bd9"
adzuna_app_key = "2475b41e24924103e08d21fd59ace651"

# Category to fetch data for
category = "it-jobs"
# Location to search in
location = "UK"

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[database_name]

# Initialize variables for pagination
page = 1
results_per_page = 50  # Adjust as needed

while True:
    # Construct the API URL for the current page
    adzuna_api_url = f"{adzuna_api_base_url}/{page}"
    
    # Set API parameters for the current page
    params = {
        'app_id': adzuna_app_id,
        'app_key': adzuna_app_key,
        'category': category,
        'results_per_page': results_per_page,
        'location0': location  # Add the location parameter
    }
    
    # Fetch data from Adzuna API
    response = requests.get(adzuna_api_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Debugging: Print the entire response to inspect its structure
        print(f"Response data: {data}")
        
        # Create a new collection in MongoDB for the category
        collection_name = category.replace("-", "_")
        collection = db[collection_name]
        
        # Store data in MongoDB
        if 'results' in data:
            num_results = len(data['results'])
            
            for job in data['results']:
                # Debugging: Print the location object for each job
                print(f"Job ID: {job['id']}, Location: {job.get('location')}")
                
                collection.update_one(
                    {'id': job['id']},  # Ensure unique documents
                    {'$set': job},  # Update with new data
                    upsert=True  # Insert if not present
                )
            print(f"Page {page}: Data for category '{category}' successfully stored in MongoDB collection '{collection_name}'")
            
            # Check if there are fewer results than the results_per_page
            if num_results < results_per_page:
                break  # Exit loop if fewer results than results_per_page
            page += 1  # Move to the next page
        else:
            print(f"No results found for category '{category}'")
            break  # Exit loop if no results
    else:
        print(f"Failed to fetch data for category '{category}': {response.status_code}")
        break  # Exit loop on failure

# Close MongoDB connection
client.close()
