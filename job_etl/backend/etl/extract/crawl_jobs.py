import requests
import json
import os
from datetime import datetime
import time
from dotenv import load_dotenv

load_dotenv()
RAPIDAPI_KEY1 = os.getenv('RAPIDAPI_KEY1')
RAPIDAPI_HOST = 'jsearch.p.rapidapi.com'

# Config
JOB_TITLE = ['Data Engineer']
JOB_TYPES = ['Remote','Onsite']
COUNTRIES = ['Vietnam','Philippines','Singapore']
NUM_PAGES = 2

def extract_job(query,location,pages=1):
    """
    Crawling list of jobs from API
    :param: query: job_title.
    :param: location: Location or type of job to search for jobs.
    :param: Pages to crawl.
    :return: List of jobs
    """
    results = []
    for page in range(1,pages + 1):
        url = f'https://{RAPIDAPI_HOST}/search?'
        # define headers, params and query
        headers = {
            'X-RapidAPI-Key' : RAPIDAPI_KEY1,
            'X-RapidAPI-Host' : RAPIDAPI_HOST
        }

        search_query = f'{query} in {location}'
        params = {
            'query' : search_query,
            'page' : str(page)
        }

        response = requests.get(url,headers=headers,params=params)
        if response.status_code == 200:
            data = response.json()
            results.extend(data.get('data',[]))
        else:
            print(f"Failed [{search_query}] page {page}: {response.status_code}")
        # Avoid rate limit
        time.sleep(1)
    return results

def save_json(data):
    """
    Save jobs to JSON files.
    :param: data: data to save.
    """
    # Get today date
    today = datetime.today().strftime('%Y_%m_%d')
    path = f'/home/thangtranquoc/job_market_etl/job_etl/backend/data/raw/raw_jobs/raw_jobs_{today}.json'
    with open(path, 'w',encoding = 'utf-8') as f:
        json.dump(data,f,indent=2)
    print(f'Saved to {path}')

def crawl_market_job():
    all_jobs = []
    for country in COUNTRIES:
        for job in JOB_TITLE:
            for job_type in JOB_TYPES:
                location = 'remote' if job_type.lower() == 'remote' else country
                print(f'Fetching: {job} | {job_type} | {country}')
                jobs = extract_job(job,location,pages=NUM_PAGES)
                all_jobs.extend(jobs)
    print(f'Total jobs collected: {len(all_jobs)}')
    save_json(all_jobs)

crawl_market_job()