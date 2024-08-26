import requests
import pymongo
from urllib.parse import urlencode

# MongoDB setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['jobtome']
collection = db['jobs']

# Jobtome API details
pid = "117-1936806-9768"
country = "uk"
results_per_page = 50
ip = "123.456.7.89"
browser = "Mozilla%2F5.0+%28Windows+NT+6.1%3B+WOW64%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+Chrome%2F27.0.1453.110+Safari%2F537.36"
channel = ""

categories = [
"Sales", "Science", "Social Work", "Sports", "Teaching", "Therapy",
    "Tourism", "Training", "Transport", "Travel", "Veterinary", "Warehouse"
]

def fetch_and_store_jobs(category):
    page = 1
    while True:
        params = {
            'pid': pid,
            'k': category,
            'ip': ip,
            'browser': browser,
            'country': country,
            'output': 'json',
            'results': results_per_page,
            'p': page
        }
        if channel:
            params['channel'] = channel

        url = f"http://partner.api.jobtome.com/v2.php?{urlencode(params)}"
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        job_offers = data.get('results', [])

        if not job_offers:
            break

        for job in job_offers:
            job_document = {
                'title': job.get('title'),
                'company': job.get('company', 'N/A'),
                'salary': job.get('salary', 'N/A'),
                'posted_date': job.get('date', 'N/A'),
                'url': job.get('url'),
                'onmousedown': job.get('onmousedown'),
                'country': country,
                'category': category,
                'location': job.get('location', 'N/A')  # Adding location to the job document
            }
            collection.update_one({'url': job_document['url']}, {'$set': job_document}, upsert=True)

        page += 1
        print(f"Page {page - 1} processed for category: {category}")

def fetch_jobs_for_all_categories():
    for category in categories:
        fetch_and_store_jobs(category)

def search_jobs(query, country='it', limit=10):
    collection.create_index([('title', pymongo.TEXT), ('description', pymongo.TEXT)])
    
    search_query = {
        '$text': {'$search': query},
        'country': country
    }
    
    jobs = collection.find(search_query).limit(limit)
    return list(jobs)

# Fetch and store job data for all categories
fetch_jobs_for_all_categories()

# Example search
search_query = "developer"
search_results = search_jobs(search_query)

for job in search_results:
    print(f"Title: {job['title']}, Company: {job['company']}, Salary: {job['salary']}, Posted Date: {job['posted_date']}, Location: {job['location']}, URL: {job['url']}")
