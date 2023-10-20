import requests
import pandas as pd
import logging as log
from tqdm import tqdm
import time
import random
from config.conf import *
import os
from datetime import datetime
import logging


logging.basicConfig(filename='log.log', level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')



####### jobsearch ############################################
def generate_url(job_title="Data Engineer", location="Türkiye", start_count=0):
    job_title = job_title_dict[job_title]
    location = geoid_dict[location]

    BASE_URL = 'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-186&count=25&q=jobSearch&query='

    QUERY = f'(origin:JOB_SEARCH_PAGE_SEARCH_BUTTON,keywords:{job_title},locationUnion:(geoId:{location}),spellCorrectionEnabled:true)&start={start_count}'

    URL = BASE_URL + QUERY

    return URL


def fetch_data(url):
    response = requests.get(url=url, cookies=cookies, headers=headers)
    response.raise_for_status()
    return response.json(), response.status_code


def json_to_dataframe_job_ids(result_json):

    included_json = result_json["included"]

    data = pd.json_normalize(included_json)

    # title
    title = data[["entityUrn","title"]].dropna()
    title = title.rename(columns={"entityUrn": "*jobPosting"})

    # company_name
    company_name = data[["*jobPosting", "primaryDescription.text"]].dropna()

    # location
    location = data[["*jobPosting", "secondaryDescription.text"]].dropna()
    df = pd.merge(title,company_name, on="*jobPosting")
    job_list_df = pd.merge(df,location, on="*jobPosting")

    return job_list_df


def fetch_job_ids(job_title, location):

    """
    Linkedin üzerinde belirli arama kriterlere göre iş ilanlarının ID bilgilerini çeker.
    Input olarak job_title ve location bilgisi verilmesi gerekir.
    """

    start_count = 0
    page_number = 0
    all_job_ids_dataframe = pd.DataFrame()
    log.info("fetch_job_ids Begins")
    try:
        while True:

            page_number += 1

            # Parametrelere göre Request URL oluşturulması.
            url = generate_url(job_title, location, start_count=start_count)

            # Request URL
            result_json, status_code = fetch_data(url)

            print(f"Job Title: {job_title}, Location: {location}, Page Number: {page_number} | Status Code: {status_code}")


            if not result_json["data"]["elements"]:
                print("No more data for Job Id search.")
                break

            try: # Veri başarıyla çekildiyse işlem yap

                job_id_dataframe = json_to_dataframe_job_ids(result_json)

                all_job_ids_dataframe = pd.concat([all_job_ids_dataframe, job_id_dataframe], ignore_index=True)

                # linkedin arama sayfasında sonuçlar 25 kayıt olacak şekilde artarak devam ediyor..
                start_count += 25


            except Exception as e:
                logging.error("ERROR:", e)


        # Transform Data
        # Column isimleri değişti.
        all_job_ids_dataframe.columns = ["job_id", "job_title", "company_name", "location"]
        all_job_ids_dataframe['job_id'] = all_job_ids_dataframe['job_id'].str.replace('urn:li:fsd_jobPosting:', '')

        # Create outputs folder if not exist
        os.makedirs("outputs", exist_ok=True)

        # Generate File Path and CSV File
        job_title = job_title.lower()
        job_title = job_title.replace(" ", "_")

        location = location.lower()

        today = datetime.today().strftime("%d-%m-%y")
        file_name = f'{job_title}_{location}_job_ids_data_{today}'
        file_path = f"outputs/{file_name}.csv"

        all_job_ids_dataframe.to_csv(file_path, index=False)

        log.info("fetch_job_ids Successful")
        return all_job_ids_dataframe

    except Exception as e:
        logging.error("Hata oluştu:", str(e))







####### jobdetail #######################
def fetch_job_details_json(cookies=cookies, headers=headers, job_id=3731588298):
    """
    Fetches details of a job posting from LinkedIn using its API.

    Args:
        cookies (dict): A dictionary containing cookies for authentication (default is 'cookies' variable).
        headers (dict): A dictionary containing headers for the HTTP request (default is 'headers' variable).
        job_id (int): The unique identifier of the job posting (default is 3731588298).

    Returns:
        tuple: A tuple containing two elements:
            1. Dictionary containing job details (retrieved from response.json()["data"]).
            2. List of included items (retrieved from response.json()["included"]).

    Raises:
        HTTPError: If the HTTP request returns an error status code.

    Example:
        data, included = fetch_job_details_json()
    """
    job_detail_response = requests.get(
        f'https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}',
        cookies=cookies,
        headers=headers,
    )

    job_detail_response.raise_for_status()

    return job_detail_response.json()["data"], job_detail_response.json()["included"]

def get_job_detail_dataframe(job_id):

    job_json, included_json = fetch_job_details_json(job_id=job_id)

    try:
        job_id = job_json["entityUrn"]
    except:
        job_id = None
    try:
        job_description = job_json["description"]["text"]
    except:
        job_description = None

    try:
        job_apply_count = job_json["applies"]
    except:
        job_apply_count = None
    try:
        expire_date = pd.to_datetime(job_json["listedAt"], unit='ms')
    except:
        expire_date = None

    try:
        employment_status = job_json["formattedEmploymentStatus"]
    except:
        employment_status = None

    try:
        experience_level = job_json["formattedExperienceLevel"]
    except:
        experience_level = None

    try:
        industries = job_json["formattedIndustries"]
    except:
        industries = None

    try:
        job_functions = job_json["formattedJobFunctions"]
    except:
        job_functions = None

    try:
        location = job_json["formattedLocation"]
    except:
        location = None

    try:
        listed_date = pd.to_datetime(job_json["listedAt"], unit='ms')
    except:
        listed_date = None

    try:
        original_listed_date = pd.to_datetime(job_json["originalListedAt"], unit="ms")
    except:
        original_listed_date = None

    try:
        title = job_json["title"]
    except:
        title = None

    try:
        views = job_json["views"]
    except:
        views = None

    try:
        is_remote = job_json["workRemoteAllowed"]
    except:
        is_remote = None

    job_id = job_id.replace("urn:li:fs_normalized_jobPosting:", "")
  # workplaceTypes bunu çözebiliyor muyuz?

    job_dict = {"job_id": [job_id],"job_description":[job_description], "job_apply_count":[job_apply_count],
              "expire_date":[expire_date], "employment_status":[employment_status],
              "experience_level":[experience_level], "industries":[industries],
              "job_functions":[job_functions], "location":[location],
              "listed_date":[listed_date], "original_listed_date":[original_listed_date],
              "title":[title], "views":[views], "is_remote":[is_remote]}


    job_detail_dataframe = pd.DataFrame(job_dict)

    return job_detail_dataframe

def job_details_to_csv(job_ids_dataframe, DEBUG=False):
    
    all_job_details_df = pd.DataFrame()
    counter = 0

    if DEBUG == True:
        job_ids_dataframe = job_ids_dataframe.head(5)

    # df ismindeki DataFrame'in satırlarında dolaşmak için tqdm kullanımı
    for _, row in tqdm(job_ids_dataframe.iterrows(), total=len(job_ids_dataframe)):
        job_id = row["job_id"]

        try:
            job_detail_df = get_job_detail_dataframe(job_id)
        except:
            print("Error:", job_id)
            time.sleep(10)
        all_job_details_df = pd.concat([all_job_details_df, job_detail_df], ignore_index=True)

        time.sleep(random.randint(0, 1))

        counter += 1

        # İlk 50 işlemde Excel dosyası oluştur. Sonrasında üstüne kaydet.
        # Oluşturamazsan 10 saniye dur.
        if counter == 50:
            try:

                os.makedirs("outputs", exist_ok=True)
                today = datetime.today().strftime("%d-%m-%y")
                file_name = f"outputs/_job_details_data_{today}.csv"
                all_job_details_df.to_csv(file_name, index=False)
                print("Excel dosyası oluşturuldu")
            except Exception as e:
                print(e)
                time.sleep(10)
            counter = 0

    return all_job_details_df
########################################




