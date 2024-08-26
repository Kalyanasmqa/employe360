import requests
import pymongo
from pymongo import MongoClient
import time

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['joblookup']
collection = db['jobs']

# API base URL
api_url = "https://joblookup.com/api/v1/jobs.json"

# Replace with your valid IP address
valid_ip_address = "192.168.1.1"  # Example of a valid IPv4 address

# Request parameters
params = {
    'publisher': 68,
    'user_ip': valid_ip_address,
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'limit': 50,  # Number of jobs per request (page size)
    'country': 'uk',
}

# List of categories
categories = [
    "Accountancy", "Administration and Secretarial", "Aerospace and Aviation", "Agriculture Fishing, Forestry and Conservation",
    "Automotive", "Banking and Financial Services", "Catering and Hospitality", "Charity and Voluntary", 
    "Construction and Property", "Customer Service", "Defence and Military", "Driving and Transport", 
    "Education and Training", "Electronics", "Energy, Utilities, Oil and Gas", "Engineering", 
    "Fashion and Arts", "FMCG", "Graduates and Trainees", "Health, Security and Safety", 
    "Healthcare and Medical", "Human Resources and Personnel", "Information Technology", "Insurance", 
    "Journalism, Publishing and Translation", "Legal", "Leisure, Tourism and Entertainment", 
    "Logistics and Warehousing", "Management and Consultancy", "Manufacturing and Production", 
    "Marketing, Advertising and PR", "Media, Design and Creative", "Personal Assistant", 
    "Pharmaceutical", "Public Sector and Government", "Purchasing and Procurement", "Recruitment", 
    "Retail and Wholesale", "Sales", "Scientific", "Social Care", "Telecommunications"
]

# List of cities
cities = [
    'london', 'birmingham', 'glasgow', 'manchester', 'leeds', 'sheffield', 'bristol', 'edinburgh',
    'liverpool', 'cardiff', 'nottingham', 'leicester', 'coventry', 'belfast', 'southampton', 
    'derby', 'oxford', 'cambridge', 'brighton', 'reading', 'hull', 'newcastle', 'plymouth',
    'norwich', 'wolverhampton', 'stoke-on-trent', 'exeter', 'preston', 'warrington', 'luton',
    'wakefield', 'swindon', 'milton-keynes', 'portsmouth', 'colchester', 'maidstone', 
    'slough', 'harlow', 'rochdale', 'halifax', 'basingstoke', 'doncaster', 'grimsby', 'rotherham',
    'barnsley', 'salford', 'bournemouth', 'blackpool', 'wigan', 'newport', 'aberdeen', 
    'inverness', 'perth', 'dunfermline', 'stirling', 'livingston', 'falkirk', 'dudley', 
    'hartlepool', 'darlington', 'middlesbrough', 'redcar', 'sunderland', 'gateshead', 
    'ashton-under-lyne', 'tonbridge', 'maidstone', 'eastbourne', 'shoreham-by-sea'
]

# Function to fetch and insert all data for a category and city with retries
def fetch_and_insert_all_data():
    for category in categories:
        for city in cities:
            page = 1
            total_jobs_inserted = 0

            while True:
                params['categories'] = category
                params['location'] = city
                params['page'] = page
                try:
                    response = requests.get(api_url, params=params, timeout=10)
                    response.raise_for_status()  # Raise an exception for HTTP errors

                    job_data = response.json().get('data', [])
                    if job_data:
                        # Create a set of existing URLs to prevent duplication
                        existing_urls = set(job['url'] for job in collection.find({}, {'url': 1}))
                        
                        # Insert only new jobs
                        new_jobs = [job for job in job_data if job['url'] not in existing_urls]
                        if new_jobs:
                            collection.insert_many(new_jobs)
                            total_jobs_inserted += len(new_jobs)
                            print(f"Inserted {len(new_jobs)} jobs for category: {category}, City: {city}, Page: {page}")
                        else:
                            print(f"No new jobs found for category: {category}, City: {city}, Page: {page}")
                            break  # Exit if no new jobs are found
                        
                        page += 1
                    else:
                        print(f"No more jobs found for category: {category}, City: {city}, Total jobs inserted: {total_jobs_inserted}")
                        break  # Exit if no more jobs are returned

                except requests.exceptions.Timeout:
                    print(f"Request timed out for category: {category}, City: {city}, Page: {page}. Retrying in 5 seconds...")
                    time.sleep(5)  # Wait for 5 seconds before retrying
                except requests.exceptions.RequestException as e:
                    print(f"An error occurred: {e}")
                    break  # Exit the loop on non-recoverable error

# Call the function to fetch and insert all data
fetch_and_insert_all_data()
