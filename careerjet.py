import requests
import pymongo
from pymongo import MongoClient
import time

# Careerjet API configuration
AFFILIATE_ID = '8354f00ff118b9fb7a2b39434280594a'
LOCALE = 'en_GB'
CAREERJET_API_URL = 'http://public.api.careerjet.net/search'

# MongoDB configuration
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'careerjet_jobs'
COLLECTION_NAME = 'jobs'

# Define lists of keywords and cities
keywords = [
    'accounting-finance-jobs', 'admin-jobs', 'charity-voluntary-jobs', 'consultancy-jobs', 
    'creative-design-jobs', 'customer-services-jobs', 'domestic-help-cleaning-jobs', 
    'energy-oil-gas-jobs', 'engineering-jobs', 'graduate-jobs', 'healthcare-nursing-jobs', 
    'hospitality-catering-jobs', 'hr-jobs', 'it-jobs', 'legal-jobs', 'logistics-warehouse-jobs', 
    'maintenance-jobs', 'manufacturing-jobs', 'other-general-jobs', 'part-time-jobs', 
    'pr-advertising-marketing-jobs', 'property-jobs', 'retail-jobs', 'sales-jobs', 
    'scientific-qa-jobs', 'social-work-jobs', 'teaching-jobs', 'trade-construction-jobs', 
    'travel-jobs', 'unknown'
]

# List of UK cities
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

# Function to fetch jobs from Careerjet API
def fetch_jobs(query, location=None, pagesize=50, page=1):
    params = {
        'affid': AFFILIATE_ID,
        'keywords': query,
        'location': location,
        'pagesize': pagesize,
        'page': page,
        'locale_code': LOCALE,
        'user_ip': '0.0.0.0',
        'user_agent': 'Mozilla/5.0'
    }
    
    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        response = requests.get(CAREERJET_API_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            if 'jobs' in data and data['jobs']:
                # Print the first job to inspect its structure
                print("Sample job data:", data['jobs'][0])
            return data
        elif response.status_code == 429:  # Rate limit error
            wait_time = 2 ** retry_count  # Exponential backoff
            print(f'Rate limit hit. Retrying in {wait_time} seconds...')
            time.sleep(wait_time)
            retry_count += 1
        else:
            response.raise_for_status()
    raise Exception('Failed to fetch jobs after multiple retries')

# Function to load jobs into MongoDB
def load_jobs_to_mongo(jobs):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Define the field to use as the unique identifier
    unique_field = 'url'  # Using 'url' as the unique identifier
    
    # Create a list of job URLs to check for duplicates
    existing_urls = set(job['url'] for job in collection.find({}, {unique_field: 1}))
    
    # Filter out duplicates
    new_jobs = [job for job in jobs if job[unique_field] not in existing_urls]
    
    if new_jobs:
        result = collection.insert_many(new_jobs)
        print(f'Inserted {len(result.inserted_ids)} new jobs into MongoDB.')
    else:
        print('No new jobs to insert.')

# Main script
if __name__ == '__main__':
    total_jobs_loaded = 0

    for keyword in keywords:
        for city in cities:
            for page in range(1, 51):  # Loop through 50 pages
                print(f'Fetching jobs for Keyword: {keyword}, City: {city}, Page {page}')
                jobs_data = fetch_jobs(query=keyword, location=city, pagesize=50, page=page)
                
                if 'jobs' in jobs_data and jobs_data['jobs']:
                    jobs = jobs_data['jobs']
                    load_jobs_to_mongo(jobs)
                    total_jobs_loaded += len(jobs)
                    print(f'Keyword: {keyword}, City: {city}, Page {page} loaded with {len(jobs)} jobs.')
                    if len(jobs) < 50:  # Stop if fewer jobs are returned than requested
                        break
                else:
                    break  # Exit loop if no more jobs are returned

    print(f'Total jobs loaded: {total_jobs_loaded}')
