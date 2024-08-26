import requests
import pymongo
import time
from pymongo import MongoClient

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")
db = client["whatjobs"]
collection = db["jobs"]

# WhatJobs API configuration
api_url_base = 'https://api.whatjobs.com/api/v1/jobs.json'
publisher = '5243'
user_ip = '127.0.0.1'
user_agent = 'Mozilla/5.0'
timeout = 10  # Set a timeout for requests (in seconds)

def fetch_job_data(keyword, location, page=1):
    params = {
        'keyword': keyword,
        'location': location,
        'limit': 50,  # Set to 50 for each page
        'page': page,
        'publisher': publisher,
        'user_ip': user_ip,
        'user_agent': user_agent
    }
    
    try:
        response = requests.get(api_url_base, params=params, timeout=timeout)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx, 5xx)
        return response.json()
    except requests.exceptions.ReadTimeout:
        print(f"Request timed out for keyword '{keyword}' and location '{location}'. Retrying...")
        return None
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

def store_job_data(job_data):
    if job_data and job_data.get("data"):
        try:
            result = collection.insert_many(job_data["data"], ordered=False)  # Use ordered=False to avoid issues with duplicates
            print(f"Inserted {len(result.inserted_ids)} documents into MongoDB.")
        except Exception as e:
            print(f"Error storing job listings in MongoDB: {e}")
    else:
        print("No job data to store.")

def main():
    keywords = [""]  # Add your keywords here
    locations = [""]  # Add your locations here

    for keyword in keywords:
        for location in locations:
            page = 1
            max_retries = 3
            while True:
                for attempt in range(max_retries):
                    job_data = fetch_job_data(keyword, location, page)
                    if job_data:
                        store_job_data(job_data)
                        if len(job_data["data"]) < 50:  # Less than 50 results indicates the last page
                            print(f"Completed fetching for keyword '{keyword}' and location '{location}'.")
                            break
                        page += 1
                        break
                    else:
                        print(f"Attempt {attempt + 1}/{max_retries} failed for keyword '{keyword}' and location '{location}'. Retrying in 5 seconds...")
                        time.sleep(5)
                else:
                    print(f"Max retries reached for keyword '{keyword}' and location '{location}'. Moving to the next combination.")
                    break

if __name__ == "__main__":
    main()
