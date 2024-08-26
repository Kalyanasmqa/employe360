# Fetch XML data prem test file


import requests
import xml.etree.ElementTree as ET
from pymongo import MongoClient

# MongoDB setup
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "job_database112233"
COLLECTION_NAME = "jobs"


XML_URL = "https://www.cv-library.co.uk/cgi-bin/feed.xml?affid=106079"

def fetch_xml_data(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses
    xml_data = response.text
    print(xml_data)  # Print the raw XML data
    return xml_data

def parse_xml_data(xml_data):
    root = ET.fromstring(xml_data)
    jobs = []

    # Define the namespaces used in the XML
    namespaces = {'ns': 'http://www.cv-library.co.uk'}

    # Extract job data
    for item in root.findall('ns:job', namespaces):
        job = {
            'title': item.find('ns:title', namespaces).text,
            'description': item.find('ns:description', namespaces).text,
            'location': item.find('ns:location', namespaces).text,
            'company': item.find('ns:company', namespaces).text,
            'date': item.find('ns:date', namespaces).text,
            'url': item.find('ns:url', namespaces).text
        }
        print(f"Parsed job: {job['title']}")  # Debug print
        jobs.append(job)

    return jobs

def store_jobs_to_mongodb(jobs):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Insert job data into MongoDB
    if jobs:
        collection.insert_many(jobs)
        print(f"Inserted {len(jobs)} job records into MongoDB.")
    else:
        print("No job records found to insert.")

def main():
    print("Fetching XML data...")
    xml_data = fetch_xml_data(XML_URL)
    print("Parsing XML data...")
    jobs = parse_xml_data(xml_data)
    print("Storing data to MongoDB...")
    store_jobs_to_mongodb(jobs)
    print("Process completed.")

if __name__ == "__main__":
    main()
