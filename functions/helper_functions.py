import requests.exceptions
import pandas as pd
from tqdm import tqdm
import time
import random
from configs.conf import *
import os
from datetime import datetime
from urllib.parse import quote
from functions.logger import logger
from sqlalchemy import create_engine
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import Counter
import string
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from stop_words import get_stop_words
import re
from nltk.corpus import stopwords
from io import StringIO


def generate_url(job_title="Data Engineer", location="Turkey", start_count=0):

    job_title = quote(job_title)
    location = geoid_dict.get(location, "Bilinmeyen Konum")

    BASE_URL = 'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-186&count=25&q=jobSearch&query='

    QUERY = f'(origin:JOB_SEARCH_PAGE_SEARCH_BUTTON,keywords:{job_title},locationUnion:(geoId:{location}),spellCorrectionEnabled:true)&start={start_count}'

    URL = BASE_URL + QUERY

    logger.debug(f"Crawler URL created has been successfully")
    print(URL)
    return URL

# Veri çekme fonksiyonu
def fetch_data(url):

    print(url)
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

    today = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
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
                job_id_dataframe["job_title_search_term"] = job_title
                job_id_dataframe["location_search_term"] = location
                job_id_dataframe["load_date"] = today
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
        all_job_ids_dataframe.columns = ["job_id", "job_title", "company_name", "location", "job_title_search_term", "location_search_term", "load_date"]
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
    
    logger.info(f'Job ID data transformation process is completed: {job_id}')

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

                os.makedirs("dags/outputs", exist_ok=True)
                today = datetime.today().strftime("%d-%m-%y")

                job_title = job_title.lower()
                location = location.lower()
                file_name = f'{job_title}_{location}_job_details_data_{today}'
                file_path = f"dags/outputs/{file_name}.csv"
                all_job_details_df.to_csv(file_path, index=False)
                logger.debug(f'Excel file creation completed successfully.: {file_name}')

            except Exception as e:
                logger.error(f'An error occurred during creating Excel file: {str(e)}')
                time.sleep(10)
            counter = 0

    return all_job_details_df

def generate_job_details_csv_airflow(job_title, location, DEBUG, **kwargs):
    location = location.lower()
    logger.debug('job_details_to_csv function started.')

    all_job_details_df = pd.DataFrame()
    counter = 0

    # File path construction based on job_title and location
    today = datetime.today().strftime("%d-%m-%y")
    job_title_lower = job_title.lower().replace(" ", "_")  # "_" eklenmiş hali
    location_lower = location.lower()
    file_name = f'{job_title_lower}_{location_lower}_job_ids_data_{today}'
    file_path = f"dags/outputs/{file_name}.csv"
    job_ids_dataframe = pd.read_csv(file_path)

    if DEBUG:
        job_ids_dataframe = pd.read_csv(file_path)
        job_ids_dataframe = job_ids_dataframe.head(5)

        # DataFrame'in satırlarında dolaşmak için tqdm kullanımı
    for _, row in tqdm(job_ids_dataframe.iterrows(), total=len(job_ids_dataframe)):
        job_id = row["job_id"]

        try:
                job_detail_df = get_job_detail_dataframe(job_id)
                logger.debug(f'Job ID: {job_id}, job_detail_df completed successfully.')

        except Exception as e:
                logger.error(f'Hata oluştu: Job ID - {job_id}, Hata: {str(e)}')
                time.sleep(10)

        all_job_details_df = pd.concat([all_job_details_df, job_detail_df], ignore_index=True)
        all_job_details_df["cleaned_description"] = all_job_details_df["job_description"].fillna("").apply(
            lambda x: str(x).lower().translate(str.maketrans("", "", string.punctuation)))
        all_descriptions = ','.join(all_job_details_df["cleaned_description"])
        logger.info("Data Transformation Successed.")
        word_frequencies = Counter(all_descriptions.split())
        common_keywords = [word for word, freq in word_frequencies.most_common(50)]
        custom_stop_words = list(
            ENGLISH_STOP_WORDS.union(get_stop_words("german")).union(get_stop_words("turkish")).union(
                get_stop_words("french")).union(custom_stopwords))
        max_keywords = 100
        tfidf_vectorizer = TfidfVectorizer(max_features=max_keywords, stop_words=custom_stop_words)
        tfidf_matrix = tfidf_vectorizer.fit_transform(all_job_details_df["cleaned_description"])
        important_keywords = tfidf_vectorizer.get_feature_names_out()
        all_job_details_df["skills"] = all_job_details_df["cleaned_description"].apply(lambda x: ', '.join(
            [word for word in x.split() if
                    word in important_keywords]))
        logger.info("skills_catcher successful.")

        time.sleep(random.randint(0, 1))

        counter += 1

        # İlk 50 işlemde CSV dosyası oluştur. Sonrasında üstüne kaydet.
         # Oluşturamazsan 10 saniye dur.
        if counter == 50 or counter == len(job_ids_dataframe):
            try:
                os.makedirs("dags/outputs/", exist_ok=True)
                today = datetime.today().strftime("%d-%m-%y")

                job_title = job_title.lower()
                location = location.lower()
                file_name = f'{job_title}_{location}_job_details_data_{today}'
                file_path = f"dags/outputs/{file_name}.csv"
                all_job_details_df.to_csv(file_path, index=False)
                logger.debug(f'CSV file creation completed successfully.: {file_name}')

            except Exception as e:
                logger.error(f'An error occurred during creating CSV file: {str(e)}')
                time.sleep(10)
            counter = 0

    return file_path

def fetch_job_ids_airflow(job_title, location, DEBUG, **kwargs):
    """
    Linkedin üzerinde belirli arama kriterlere göre iş ilanlarının ID bilgilerini çeker.
    Input olarak job_title ve location bilgisi verilmesi gerekir.
    """

    today = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
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

            logger.info(
                f"Job Title: {job_title}, Location: {location}, Total Job Queried: {total_job_count}, Page Number: {page_number}")

            if not result_json["data"]["elements"]:
                logger.info("No additional data available for Job ID search.")
                break

            try:
                # Veri başarıyla çekildiyse işlem yap
                job_id_dataframe = json_to_dataframe_job_ids(result_json)
                job_id_dataframe["job_title_search_term"] = job_title
                job_id_dataframe["location_search_term"] = location
                job_id_dataframe["load_date"] = today
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
        all_job_ids_dataframe.columns = ["job_id", "job_title", "company_name", "location", "job_title_search_term",
                                         "location_search_term", "load_date"]
        all_job_ids_dataframe['job_id'] = all_job_ids_dataframe['job_id'].str.replace('urn:li:fsd_jobPosting:', '')
        logger.debug("Job ID Dataframe data transformation process is succeed.")

        # Var olan bir klasör oluştur
        os.makedirs("dags/outputs/", exist_ok=True)

        # Dosya Yolu Oluştur ve CSV Dosyasını Kaydet
        job_title = job_title.lower()
        job_title = job_title.replace(" ", "_")
        location = location.lower()

        today = datetime.today().strftime("%d-%m-%y")
        file_name = f'{job_title}_{location}_job_ids_data_{today}'
        file_path = f"dags/outputs/{file_name}.csv"

        # Eğer dosya varsa, varolan veriye ekleyin
        if os.path.exists(file_path):
            existing_data = pd.read_csv(file_path)
            all_job_ids_dataframe = pd.concat([existing_data, all_job_ids_dataframe], ignore_index=True)

        all_job_ids_dataframe.to_csv(file_path, index=False)
        crawled_job_id_count = len(all_job_ids_dataframe)
        total_job_count = result_json["data"]["paging"]["total"]
        logger.info(f"{crawled_job_id_count} of {total_job_count} crawled.")
        return file_path, job_title

    except Exception as e:
        # Hata günlüğüne hata mesajı ve ayrıntıları ekle
        logger.error("An error occurred while fetch_job_ids", str(e))

        # Veriyi Dönüştür
        all_job_ids_dataframe.columns = ["job_id", "job_title", "company_name", "location", "job_title_search_term",
                                         "location_search_term", "load_date"]
        all_job_ids_dataframe['job_id'] = all_job_ids_dataframe['job_id'].str.replace('urn:li:fsd_jobPosting:', '')
        logger.debug("Job ID Dataframe data transformation process is succeed.")

        # Var olan bir klasör oluştur
        os.makedirs("dags/outputs", exist_ok=True)

        # Dosya Yolu Oluştur ve CSV Dosyasını Kaydet
        job_title = job_title.lower()
        job_title = job_title.replace(" ", "_")
        location = location.lower()

        today = datetime.today().strftime("%d-%m-%y")
        file_name = f'{job_title}_{location}_job_ids_data_{today}'
        file_path = f"dags/outputs/{file_name}.csv"

        all_job_ids_dataframe.to_csv(file_path, index=False)
        crawled_job_id_count = len(all_job_ids_dataframe)
        total_job_count = result_json["data"]["paging"]["total"]
        logger.info(f"{crawled_job_id_count} of {total_job_count} crawled.")

        return file_path

    except Exception as e:
        # Hata günlüğüne hata mesajı ve ayrıntıları ekle
        logger.error("An error occurred while fetch_job_ids", str(e))

def upload_to_gcs(data_folder,  **kwargs):
    data_folder = "dags/outputs/"
    bucket_name = 'linkedin-job'
    gcs_conn_id = 'google_cloud_connection'

    csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]

    for csv_file in csv_files:
        local_file_path = os.path.join(data_folder, csv_file)
        gcs_file_path = f"Jobs/{csv_file}"

        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_to_gcs',
            src=local_file_path,
            dst=gcs_file_path,
            bucket=bucket_name,
            gcp_conn_id=gcs_conn_id,
        )
        upload_task.execute(context=kwargs)

def check_csv_files_existence(data_folder, **kwargs):
    try:
        csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]
        return 'upload_to_gcs' if csv_files else 'no_op'
    except Exception as e:
        print(f"An error occurred while checking CSV files existence: {str(e)}")
        return 'no_op'

def delete_csv_files(data_folder, **kwargs):
    try:
        csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]

        for csv_file in csv_files:
            file_path = os.path.join(data_folder, csv_file)
            os.remove(file_path)
            print(f"Deleted file: {file_path}")

    except Exception as e:
        print(f"An error occurred while deleting CSV files: {str(e)}")
########################################


def create_postgresql_connection(DB_USERNAME, DB_PASSWORD, DB_HOST_IP, DB_NAME):
    con = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST_IP}:5432/{DB_NAME}"
    engine = create_engine(con)
    return engine


def skills_catcher(DataFrame):
    def process_description(description):
        try:
            if pd.notna(description):
                # Noktalama işaretlerini ve sayıları temizleme
                cleaned_text = re.sub(r'[^\w\s]', '', description)
                cleaned_text = re.sub(r'\d', '', cleaned_text)

                # Küçük harfe çevirme
                cleaned_text = cleaned_text.lower()

                # Stopwords'leri çıkarma
                stop_words = set(stopwords.words(["english", "german", "french", "turkish"]))
                cleaned_text = " ".join(word for word in cleaned_text.split() if word not in stop_words)

                logger.info("Description processed successfully.")
                return cleaned_text
            else:
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f'Error_Code:{str(e)}')
            return None

    def process_skills(description):
        try:
            if pd.notna(description):
                skills = ", ".join(set(
                    "power bi" if word == "bi" or (word.startswith("bi") and not word[2:].isalpha()) else word for word
                    in
                    description.split() if word in target_keywords))
                skills = skills.replace("powerpower bi", "power bi")
                skills = skills.replace("power power bi", "power bi")
                skills = skills.replace("power bigquery", "bigquery")
                skills = skills.replace("powerbi", "power bi")
                logger.info("Skills processed successfully.")
                return skills
            else:
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f'Error_Code:{str(e)}')
            return None

    DataFrame["cleaned_description"] = DataFrame["job_description"].apply(process_description)

    DataFrame["skills"] = DataFrame["cleaned_description"].apply(process_skills)

    return DataFrame

def skills_catcher_db(DataFrame):
    logger.info("skills_catcher started.")
    DataFrame["cleaned_description"] = DataFrame["job_description"].fillna("").apply(
        lambda x: str(x).lower().translate(str.maketrans("", "", string.punctuation)))
    all_descriptions = ','.join(DataFrame["cleaned_description"])
    logger.info("Data Transformation Successed.")
    word_frequencies = Counter(all_descriptions.split())
    common_keywords = [word for word, freq in word_frequencies.most_common(50)]
    custom_stop_words = list(ENGLISH_STOP_WORDS.union(get_stop_words("german")).union(get_stop_words("turkish")).union(get_stop_words("french")).union(custom_stopwords))
    max_keywords = 100
    tfidf_vectorizer = TfidfVectorizer(max_features=max_keywords, stop_words=custom_stop_words)
    tfidf_matrix = tfidf_vectorizer.fit_transform(DataFrame["cleaned_description"])
    important_keywords = tfidf_vectorizer.get_feature_names_out()
    logger.info("skills_catcher successful.")
    DataFrame["skills"] = DataFrame["cleaned_description"].apply(lambda x: ', '.join(
        set([word.replace("powerbi", "power bi").replace("bi", "power bi") for word in x.split() if
             word in important_keywords])))
    DataFrame["skills"] = DataFrame["skills"].apply(lambda x: x.replace("power power bi", "power bi"))
    DataFrame["skills"] = DataFrame["skills"].apply(lambda x: x.replace("power bigquery", "bigquery"))

    return DataFrame
