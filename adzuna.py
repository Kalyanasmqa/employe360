import requests
from pymongo import MongoClient
import time

# MongoDB connection settings
mongo_uri = "mongodb://localhost:27017/"
database_name = "adzuna"

# Adzuna API settings
adzuna_api_base_url = "https://api.adzuna.com/v1/api/jobs/gb/search"
adzuna_app_id = "25ad7bd9"
adzuna_app_key = "2475b41e24924103e08d21fd59ace651"

# List of job categories
categories = [
    "accounting-finance-jobs", "admin-jobs", "charity-voluntary-jobs", "consultancy-jobs",
    "creative-design-jobs", "customer-services-jobs", "domestic-help-cleaning-jobs",
    "energy-oil-gas-jobs", "engineering-jobs", "graduate-jobs", "healthcare-nursing-jobs",
    "hospitality-catering-jobs", "hr-jobs", "it-jobs", "legal-jobs", "logistics-warehouse-jobs",
    "maintenance-jobs", "manufacturing-jobs", "other-general-jobs", "part-time-jobs",
    "pr-advertising-marketing-jobs", "property-jobs", "retail-jobs", "sales-jobs",
    "scientific-qa-jobs", "social-work-jobs", "teaching-jobs", "trade-construction-jobs",
    "travel-jobs", "unknown"
]

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[database_name]

for category in categories:
    print(f"Processing category: {category}")
    
    # Initialize variables for pagination
    page = 1
    results_per_page = 10  # Number of results per page

    while True:
        # Construct the API URL for the current page
        adzuna_api_url = f"{adzuna_api_base_url}/{page}"
        
        # Set API parameters for the current page
        params = {
            'app_id': adzuna_app_id,
            'app_key': adzuna_app_key,
            'results_per_page': results_per_page,
            'location0': 'UK',  # Set location to UK
            'what': category,  # Job category
            'salary_min': 0,  # Minimum salary
            'content-type': 'application/json'
        }
        
        try:
            # Fetch data from Adzuna API
            response = requests.get(adzuna_api_url, params=params)
            
            # Check for HTTP errors
            response.raise_for_status()

            # Check for rate limit in headers
            if 'X-RateLimit-Limit' in response.headers:
                print(f"Rate Limit: {response.headers['X-RateLimit-Limit']}")
                print(f"Rate Limit Remaining: {response.headers['X-RateLimit-Remaining']}")
                print(f"Rate Limit Reset: {response.headers.get('X-RateLimit-Reset', 'N/A')}")
            
            # Check if the rate limit has been exceeded
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))  # Default to 60 seconds if not provided
                print(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
                continue  # Retry the same page after the delay
            
            # Parse the JSON response
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
                    collection.update_one(
                        {'id': job['id']},  # Ensure unique documents
                        {'$set': job},  # Update with new data
                        upsert=True  # Insert if not present
                    )
                print(f"Page {page}: Data for category '{category}' successfully stored in MongoDB collection '{collection_name}'")
                
                # Check if there are fewer results than the results_per_page
                if num_results < results_per_page:
                    print(f"No more results for category '{category}'.")
                    break  # Exit loop if fewer results than results_per_page
            else:
                print(f"No results found for category '{category}'")
                break  # Exit loop if no results
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            break
        except requests.exceptions.RequestException as req_err:
            print(f"Error occurred during the request: {req_err}")
            break

        # Move to the next page
        page += 1

        # Add a delay between requests to avoid rate limiting
        time.sleep(1)

# Close MongoDB connection
client.close()
print("Data retrieval complete for all categories.")
