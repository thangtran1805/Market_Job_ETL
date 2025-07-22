import requests
import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv

def extract_job_salary(job_titles,locations):
    load_dotenv()
    RAPIDAPI_KEY3 = os.getenv('RAPIDAPI_KEY3')
    RAPIDAPI_HOST = 'jsearch.p.rapidapi.com'
    HEADERS = {
        'X-RapidAPI-Key': RAPIDAPI_KEY3,
        'X-RapidAPI-host': RAPIDAPI_HOST
    }
    results = []
    for title in job_titles:
        for location in locations:
            search_query = {'job_title': title, 'location' : location}
            url = f'https://{RAPIDAPI_HOST}/estimated-salary?'
            try:
                response = requests.get(url,headers=HEADERS,params=search_query)
                if response.status_code == 200:
                    data = response.json().get('data',[])
                    if data:
                        for item in data:
                            item['job_title'] = title
                            item['location'] = location
                            results.append(data)
                else:
                    print(f'Failed to get salary for {title} in {location}')
            except Exception as e:
                print(f'Error fetching salary for {title} in {location} : {e}')
            # avoid rate limit
            time.sleep(1)
    return results

def save_json(data,path):
    """
    Save data to a JSON file.
    :param: data: The data to save.
    :param: path: Path to the output file.
    """
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f'Saved job salary to: {path}')        

def crawl_jobs_salary(date_str = None):
    """
    Main ETL function to crawl job salaries from job_title and location.
    :param: date_str: Optional date string (format: YYYY_MM_DD) for reruns.
    """
    job_titles = ['Data Engineer']
    locations = ['Vietnam','Philippines','Singapore']
    salary_data = extract_job_salary(job_titles,locations)
    # Define output path
    output_path_dir = f'/home/thangtranquoc/job_market_etl/job_etl/backend/data/raw/raw_jobs_salary'
    if not date_str:
        date_str = datetime.today().strftime('%Y_%m_%d')
    output_file = f'raw_jobs_salary_{date_str}.json'
    output_path = os.path.join(output_path_dir, output_file)

    # Save to JSON
    save_json(salary_data, output_path)
crawl_jobs_salary()

