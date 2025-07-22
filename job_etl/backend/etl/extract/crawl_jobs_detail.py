import requests
import json
import os
from datetime import datetime
import time
from dotenv import load_dotenv

def get_latest_file_in_directory(extension, directory):
    """
    Get the latest file in a directory.
    :param: extension: File's extension.
    :param: directory: A directory to search for files.
    :return: Path to the latest file or None if no files were found.
    """
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def save_json(data, path):
    """
    Save data to a JSON file.
    :param: data: The data to save.
    :param: path: Path to the output file.
    """
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f'Saved job details to: {path}')

def extract_job_details(job_ids):
    """
    Crawl job details from job_id list.
    :param: job_ids: List of job_id.
    :return: List of job detail objects.
    """
    load_dotenv()
    RAPIDAPI_KEY2 = os.getenv('RAPIDAPI_KEY2')
    RAPIDAPI_HOST = 'jsearch.p.rapidapi.com'
    HEADERS = {
        'X-RapidAPI-Key': RAPIDAPI_KEY2,
        'X-RapidAPI-host': RAPIDAPI_HOST
    }

    results = []
    for job_id in job_ids:
        url = f'https://{RAPIDAPI_HOST}/job-details?'
        search_query = {'job_id': job_id}
        retries = 3

        for attempt in range(retries):
            try:
                response = requests.get(url, headers=HEADERS, params=search_query)
                if response.status_code == 200:
                    job_detail = response.json()['data']
                    results.append(job_detail)
                    break
                elif response.status_code == 429:
                    print(f'[Retry] Rate limited for {job_id}. Retrying...')
                    time.sleep(2 ** attempt)
                else:
                    print(f'[Warning] Failed to get job details for {job_id}, status code: {response.status_code}')
                    break
            except Exception as e:
                print(f'[Error] Exception for {job_id}: {e}')
                time.sleep(1)
        time.sleep(1)  # rate limit friendly
    return results

def crawl_jobs_detail(date_str=None):
    """
    Main ETL function to crawl job details from the latest job file.
    :param: date_str: Optional date string (format: YYYY_MM_DD) for reruns.
    """
    extension = '.json'
    input_path = '/home/thangtranquoc/job_market_etl/job_etl/backend/data/raw/raw_jobs'
    output_path_dir = '/home/thangtranquoc/job_market_etl/job_etl/backend/data/raw/raw_jobs_detail'

    latest_file = get_latest_file_in_directory(extension, input_path)
    if latest_file is None:
        raise FileNotFoundError(f"No JSON files found in {input_path}")

    with open(latest_file, 'r', encoding='utf-8') as f:
        jobs = json.load(f)

    # Extract job_id
    job_ids = [job['job_id'] for job in jobs if 'job_id' in job]

    if not job_ids:
        print("[Info] No job_ids found in latest job file.")
        return

    # Crawl job details
    details = extract_job_details(job_ids)

    # Define output path
    if not date_str:
        date_str = datetime.today().strftime('%Y_%m_%d')
    output_file = f'raw_jobs_detail_{date_str}.json'
    output_path = os.path.join(output_path_dir, output_file)

    # Save to JSON
    save_json(details, output_path)

crawl_jobs_detail()
