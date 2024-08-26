import requests
import xml.etree.ElementTree as ET
from pymongo import MongoClient

# MongoDB setup
client = MongoClient('mongodb://localhost:27017/')
db = client['cvlibrary']  # Replace with your database name
collection = db['jobs']      # Replace with your collection name

# Fetch the XML data
url = "https://www.cv-library.co.uk/cgi-bin/feed.xml?affid=106079"
response = requests.get(url)
xml_data = response.content

# Parse the XML data
root = ET.fromstring(xml_data)

# Extract and insert job data into MongoDB
jobs = []
for job in root.findall('job'):
    job_data = {
        'jobref': job.find('jobref').text,
        'date': job.find('date').text,
        'title': job.find('title').text,
        'company': job.find('company').text,
        'email': job.find('email').text,
        'url': job.find('url').text,
        'salarymin': job.find('salarymin').text,
        'salarymax': job.find('salarymax').text,
        'benefits': job.find('benefits').text,
        'salary': job.find('salary').text,
        'jobtype': job.find('jobtype').text,
        'full_part': job.find('full_part').text,
        'salary_per': job.find('salary_per').text,
        'location': job.find('location').text,
        'city': job.find('city').text,
        'county': job.find('county').text,
        'country': job.find('country').text,
        'description': job.find('description').text,
        'category': job.find('category').text,
        'image': job.find('image').text
    }
    jobs.append(job_data)

# Insert data into MongoDB
collection.insert_many(jobs)

print("Data inserted successfully.")
