import logging

import requests
import requests.exceptions  # requests modülünden RequestException'ı içe aktarın
import pandas as pd
from tqdm import tqdm
import time
import random
from config.conf import *
import os
from datetime import datetime
from urllib.parse import quote
from logger import logger



def generate_url(job_title="Data Engineer", location="Türkiye", start_count=0):

    job_title = quote(job_title)
    location = geoid_dict.get(location, "Bilinmeyen Konum")

    BASE_URL = 'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-186&count=25&q=jobSearch&query='

    QUERY = f'(origin:JOB_SEARCH_PAGE_SEARCH_BUTTON,keywords:{job_title},locationUnion:(geoId:{location}),spellCorrectionEnabled:true)&start={start_count}'

    URL = BASE_URL + QUERY

    logger.debug(f"Crawler URL created has been successfully")

    return URL

# Veri çekme fonksiyonu
def fetch_data(url):

    try:
        response = requests.get(url=url, cookies=cookies, headers=headers)
        response.raise_for_status()
        logger.debug(f"Data retrieval successful. Response status code: {response.status_code}")
        return response.json()
    except requests.exceptions.RequestException as e:
        # Hata günlüğüne hata mesajı ve ayrıntıları ekle
        logger.error(f"An error occurred during Job ID's crawling.{str(e)}")
        return None


def json_to_dataframe_job_ids(result_json):
    logger.debug("Started JSON-to-DataFrame conversion process.")
    try:
        included_json = result_json["included"]
        data = pd.json_normalize(included_json)

        # title
        title = data[["entityUrn","title"]].dropna()
        title = title.rename(columns={"entityUrn": "*jobPosting"})

        # company_name
        company_name = data[["*jobPosting", "primaryDescription.text"]].dropna()

        # location
        location = data[["*jobPosting", "secondaryDescription.text"]].dropna()
        df = pd.merge(title, company_name, on="*jobPosting")
        job_list_df = pd.merge(df, location, on="*jobPosting")

        logger.info('JSON-to-DataFrame processed successfully')  # İşlem başarıyla tamamlandığında bir INFO mesajı ekle
        return job_list_df
    except Exception as e:
        # Hata günlüğüne hata mesajı ve ayrıntıları ekle
        logger.error(f'An error occurred during JSON-to-DataFrame conversion process.: {str(e)}')
        return None


def fetch_job_ids(job_title, location, DEBUG):

    """
        Linkedin üzerinde belirli arama kriterlere göre iş ilanlarının ID bilgilerini çeker.
        Input olarak job_title ve location bilgisi verilmesi gerekir.
    """
    location = location.lower()
    try:
        start_count = 0
        page_number = 0
        all_job_ids_dataframe = pd.DataFrame()
        logger.debug("fetch_job_ids Process Starting")

        while True:
            page_number += 1

            # Parametrelere göre Request URL oluşturulması.
            url = generate_url(job_title, location, start_count=start_count)
            # Request URL
            result_json = fetch_data(url)
            total_job_count = result_json["data"]["paging"]["total"]

            logger.info(f"Job Title: {job_title}, Location: {location}, Total Job Queried: {total_job_count}, Page Number: {page_number}")

            if not result_json["data"]["elements"]:
                logger.info("No additional data available for Job ID search.")
                break

            try:
                # Veri başarıyla çekildiyse işlem yap
                job_id_dataframe = json_to_dataframe_job_ids(result_json)
                all_job_ids_dataframe = pd.concat([all_job_ids_dataframe, job_id_dataframe], ignore_index=True)
                logger.debug("All Job ID's dataframe created and appended succesfully.")
                # linkedin arama sayfasında sonuçlar 25 kayıt olacak şekilde artarak devam ediyor.
                start_count += 25
            except Exception as e:
                # Hata günlüğüne hata mesajı ve ayrıntıları ekle
                logger.error("An error occurred while Job ID dataframe creation", str(e))


            if DEBUG:
                logger.debug("DEBUG mode active. Crawled only the first page.")
                break

        # Veriyi Dönüştür
        all_job_ids_dataframe.columns = ["job_id", "job_title", "company_name", "location"]
        all_job_ids_dataframe['job_id'] = all_job_ids_dataframe['job_id'].str.replace('urn:li:fsd_jobPosting:', '')
        logger.debug("Job ID Dataframe data transformation process is succeed.")
        # Var olan bir klasör oluştur
        os.makedirs("outputs", exist_ok=True)

        # Dosya Yolu Oluştur ve CSV Dosyasını Kaydet
        job_title = job_title.lower()
        job_title = job_title.replace(" ", "_")

        location = location.lower()

        today = datetime.today().strftime("%d-%m-%y")
        file_name = f'{job_title}_{location}_job_ids_data_{today}'
        file_path = f"outputs/{file_name}.csv"

        all_job_ids_dataframe.to_csv(file_path, index=False)
        crawled_job_id_count = len(all_job_ids_dataframe)
        total_job_count = result_json["data"]["paging"]["total"]
        logger.info(f"{crawled_job_id_count} of {total_job_count} crawled.")
        return all_job_ids_dataframe

    except Exception as e:
        # Hata günlüğüne hata mesajı ve ayrıntıları ekle
        logger.error("An error occurred while fetch_job_ids", str(e))


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
    # Debug: Job ID bilgisi loglanıyor.
    logger.debug(f'Job ID: {job_id}')

    try:
        job_detail_response = requests.get(
            f'https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}',
            cookies=cookies,
            headers=headers,
        )

        job_detail_response.raise_for_status()

        # Debug: Başarılı bir şekilde veri çekildiğini logla.
        logger.debug(f'Veri çekme başarılı')

        return job_detail_response.json()

    except requests.exceptions.RequestException as e:
        # Hata günlüğüne hata mesajı ve ayrıntıları ekle.
        logger.error(f'An error occurred during detailed Job information. Job ID is: {job_id}. Error code: {str(e)}')
        return None


def get_job_detail_dataframe(job_id):

    logger.debug(f'Job ID ile işlem başladı: {job_id}')

    job_details_json = fetch_job_details_json(job_id=job_id)
    job_json = job_details_json["data"]
    included_json = job_details_json["included"]

    logger.info(f"Successfully crawled and retrieved detailed information for Job ID: {job_id}.")

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

    today = datetime.today().strftime("%Y-%m-%d %H:%M:%S")


    job_id = job_id.replace("urn:li:fs_normalized_jobPosting:", "")

    job_dict = {"job_id": [job_id], "job_description": [job_description], "job_apply_count": [job_apply_count],
                "expire_date": [expire_date], "employment_status": [employment_status],
                "experience_level": [experience_level], "industries": [industries],
                "job_functions": [job_functions], "location": [location],
                "listed_date": [listed_date], "original_listed_date": [original_listed_date],
                "title": [title], "views": [views], "is_remote": [is_remote], "load_date": [today]}

    logger.debug(f'Job ID: {job_id}, DataFrame oluşturuldu.')

    job_detail_dataframe = pd.DataFrame(job_dict)

    return job_detail_dataframe


def generate_job_details_csv(job_ids_dataframe, job_title, location, DEBUG):

    location = location.lower()
    logger.debug('job_details_to_csv fonksiyonu çalışıyor.')


    all_job_details_df = pd.DataFrame()
    counter = 0

    if DEBUG:
        job_ids_dataframe = job_ids_dataframe.head(5)


    # DataFrame'in satırlarında dolaşmak için tqdm kullanımı
    for _, row in tqdm(job_ids_dataframe.iterrows(), total=len(job_ids_dataframe)):
        job_id = row["job_id"]

        try:
             job_detail_df = get_job_detail_dataframe(job_id)
             logger.debug(f'Job ID: {job_id}, job_detail_df başarıyla oluşturuldu.')

        except Exception as e:
            logger.error(f'Hata oluştu: Job ID - {job_id}, Hata: {str(e)}')
            time.sleep(10)

        all_job_details_df = pd.concat([all_job_details_df, job_detail_df], ignore_index=True)

        time.sleep(random.randint(0, 1))

        counter += 1

        # İlk 50 işlemde Excel dosyası oluştur. Sonrasında üstüne kaydet.
        # Oluşturamazsan 10 saniye dur.
        if counter == 50 or counter == len(job_ids_dataframe):
            try:

                os.makedirs("outputs", exist_ok=True)
                today = datetime.today().strftime("%d-%m-%y")

                job_title = job_title.lower()
                location = location.lower()
                file_name = f'{job_title}_{location}_job_details_data_{today}'
                file_path = f"outputs/{file_name}.csv"
                all_job_details_df.to_csv(file_path, index=False)
                logger.debug(f'Excel file creation completed successfully.: {file_name}')

            except Exception as e:
                logger.error(f'An error occurred during creating Excel file: {str(e)}')
                time.sleep(10)
            counter = 0

    return all_job_details_df
########################################


def create_postgresql_connection():
    con = f"postgresql://{user_name}:{password}@{host_ip}:{host_port}/{database_name}"
    engine = create_engine(con)
    return engine
def export_postgresql(dataframe, table_name):

    engine = create_postgresql_connection()
    try:
        dataframe.to_sql(f"{table_name}", con=engine , index=False, if_exists='append')
        logging.info("Data has been successfully transferred to the database.")
    except Exception as e:
        raise logging.error("An error occurred while transfer jobs data")
