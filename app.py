from flask import Flask, render_template, request
from pymongo import MongoClient
from math import ceil

app = Flask(__name__)

# MongoDB configuration
MONGO_URI = 'mongodb://localhost:27017'

# Database configurations
DATABASES = {
    'careerjet': {
        'db_name': 'careerjet_jobs',
        'collection': 'jobs'
    },
    'adzuna': {
        'db_name': 'adzuna',
        'collections': ['it_jobs', 'accounting_finance_jobs']
    },
    'whatjobs': {
        'db_name': 'whatjobs',
        'collection': 'jobs'
    },
    'cv_library': {
        'db_name': 'cvlibrary',
        'collection': 'jobs'
    },
    'jobtome': {
        'db_name': 'jobtome',
        'collection': 'jobs'
    },
    'joblookup': {
        'db_name': 'joblookup',
        'collection': 'jobs'
    }
}

client = MongoClient(MONGO_URI)

def fetch_jobs(db_name, collection_name, query=None, location=None, page=1, per_page=4):
    db = client[db_name]
    collection = db[collection_name]

    mongo_query = {}
    if query:
        mongo_query['title'] = {'$regex': query, '$options': 'i'}
    
    if location:
        if db_name == DATABASES['adzuna']['db_name']:
            mongo_query['location.area'] = {'$regex': location, '$options': 'i'}
        elif db_name in [DATABASES['whatjobs']['db_name'], DATABASES['cv_library']['db_name'], DATABASES['jobtome']['db_name'], DATABASES['joblookup']['db_name']]:
            mongo_query['location'] = {'$regex': location, '$options': 'i'}
        else:
            mongo_query['locations'] = {'$regex': location, '$options': 'i'}

    skip = (page - 1) * per_page
    jobs_cursor = collection.find(mongo_query).skip(skip).limit(per_page)
    jobs = list(jobs_cursor)

    return jobs, collection.count_documents(mongo_query)

def fetch_adzuna_jobs(query=None, location=None, page=1, per_page=4):
    combined_jobs = []
    total_jobs_count = 0

    for collection_name in DATABASES['adzuna']['collections']:
        jobs, count = fetch_jobs(
            DATABASES['adzuna']['db_name'], 
            collection_name, 
            query=query, 
            location=location, 
            page=page, 
            per_page=per_page
        )
        combined_jobs.extend(jobs)
        total_jobs_count += count

    combined_jobs = sorted(combined_jobs, key=lambda x: x.get('date', ''), reverse=True)

    return combined_jobs[:per_page], total_jobs_count

def fetch_careerjet_jobs(query=None, location=None, page=1, per_page=4):
    return fetch_jobs(
        DATABASES['careerjet']['db_name'],
        DATABASES['careerjet']['collection'],
        query=query,
        location=location,
        page=page,
        per_page=per_page
    )

def fetch_whatjobs_jobs(query=None, location=None, page=1, per_page=4):
    return fetch_jobs(
        DATABASES['whatjobs']['db_name'],
        DATABASES['whatjobs']['collection'],
        query=query,
        location=location,
        page=page,
        per_page=per_page
    )

def fetch_cv_library_jobs(query=None, location=None, page=1, per_page=4):
    return fetch_jobs(
        DATABASES['cv_library']['db_name'],
        DATABASES['cv_library']['collection'],
        query=query,
        location=location,
        page=page,
        per_page=per_page
    )

def fetch_jobtome_jobs(query=None, location=None, page=1, per_page=4):
    return fetch_jobs(
        DATABASES['jobtome']['db_name'],
        DATABASES['jobtome']['collection'],
        query=query,
        location=location,
        page=page,
        per_page=per_page
    )

def fetch_joblookup_jobs(query=None, location=None, page=1, per_page=4):
    return fetch_jobs(
        DATABASES['joblookup']['db_name'],
        DATABASES['joblookup']['collection'],
        query=query,
        location=location,
        page=page,
        per_page=per_page
    )

@app.route('/', methods=['GET'])
def index():
    query = request.args.get('query')
    location = request.args.get('location')
    page = int(request.args.get('page', 1))
    per_page = 2  # Number of results per page

    # Fetch jobs and totals
    adzuna_jobs, total_adzuna_jobs = fetch_adzuna_jobs(query=query, location=location, page=page, per_page=per_page)
    careerjet_jobs, total_careerjet_jobs = fetch_careerjet_jobs(query=query, location=location, page=page, per_page=per_page)
    whatjobs_jobs, total_whatjobs_jobs = fetch_whatjobs_jobs(query=query, location=location, page=page, per_page=per_page)
    cv_library_jobs, total_cv_library_jobs = fetch_cv_library_jobs(query=query, location=location, page=page, per_page=per_page)
    jobtome_jobs, total_jobtome_jobs = fetch_jobtome_jobs(query=query, location=location, page=page, per_page=per_page)
    joblookup_jobs, total_joblookup_jobs = fetch_joblookup_jobs(query=query, location=location, page=page, per_page=per_page)

    # Combine all jobs
    all_jobs = adzuna_jobs + careerjet_jobs + whatjobs_jobs + cv_library_jobs + jobtome_jobs + joblookup_jobs
    total_jobs_available = total_adzuna_jobs + total_careerjet_jobs + total_whatjobs_jobs + total_cv_library_jobs + total_jobtome_jobs + total_joblookup_jobs
    total_jobs_shown = len(all_jobs)

    # Calculate total pages
    total_pages = ceil(total_jobs_available / per_page)

    # Define the range of pages to show
    page_range = 2  # Number of pages to show around the current page
    start_page = max(1, page - page_range)
    end_page = min(total_pages, page + page_range)

    # Ensure there are always at least some pages shown
    if end_page - start_page < 2 * page_range:
        if start_page == 1:
            end_page = min(total_pages, end_page + (2 * page_range - (end_page - start_page)))
        elif end_page == total_pages:
            start_page = max(1, start_page - (2 * page_range - (end_page - start_page)))

    # Prepare title keyword display
    search_keywords = []
    if query:
        search_keywords.append(query)
    if location:
        search_keywords.append(location)
    search_title = " ".join(search_keywords)

    return render_template(
        'index.html',
        search_title=search_title,
        adzuna_jobs=adzuna_jobs,
        careerjet_jobs=careerjet_jobs,
        whatjobs_jobs=whatjobs_jobs,
        cv_library_jobs=cv_library_jobs,
        jobtome_jobs=jobtome_jobs,
        joblookup_jobs=joblookup_jobs,
        query=query,
        location=location,
        page=page,
        per_page=per_page,
        total_jobs_available=total_jobs_available,
        total_jobs_shown=total_jobs_shown,
        total_pages=total_pages,
        start_page=start_page,
        end_page=end_page
    )

@app.route('/contact', methods=['GET', 'POST'])
def contact():
    # Your contact view logic here
    return render_template('contact.html')

if __name__ == '__main__':
    app.run(debug=True)
