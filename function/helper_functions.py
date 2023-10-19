import requests
import pandas as pd
import logging as log
from tqdm import tqdm
import time
import random
from config.conf import *
import os

####### jobsearch ############################################
def generate_url(job_title="Data Engineer", geo_id="Türkiye", start_count=0):
    job_title = job_title_dict[job_title]
    geo_id = geo_id_dict[geo_id]

    BASE_URL = 'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-186&count=25&q=jobSearch&query='

    URL = f'(origin:JOB_SEARCH_PAGE_SEARCH_BUTTON,keywords:{job_title},locationUnion:(geoId:{geo_id}),spellCorrectionEnabled:true)&start={start_count}'

    return BASE_URL + URL
def fetch_data(url):
    response = requests.get(url=url, cookies=cookies, headers=headers)
    if not response.raise_for_status():
        print("Request successful. Status code:", response.status_code)
    return response.json()
def fetch_job_ids(result_json):
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
def job_list_to_csv(job_title, geo_id):
    start_count = 0
    df = pd.DataFrame()
    try:
        while True:
            url = generate_url(job_title, geo_id, start_count=start_count)
            result_json = fetch_data(url)
            if result_json is not None:  # Veri başarıyla çekildiyse işlem yap
                # yeni gelen veri eski verinin üstüne ekleniyor.
                df2 = fetch_job_ids(result_json)
                df = pd.concat([df, df2], ignore_index=True)
                # linkedin sayfa mantığı 0'dan başlayıp 25'er artmasından kaynaklı.
                start_count += 25
                if len(df2) == 0:
                    break
            else:
                print("Veri çekme başarısız oldu. İşlem sonlandırılıyor.")
                break
        # csv dosyası oluşturuldu
        df.to_csv("job_search_data.csv", index=False)
        return df
    except Exception as e:
        print("Hata oluştu:", str(e))
        # sütun isimleri değişti, jod_id tek başına alındı
        df.columns = ["job_id", "job_title", "company_name", "location"]
        df['job_id'] = df['job_id'].str.replace('urn:li:fsd_jobPosting:', '')
        # outputs klasörü oluştur.
        os.makedirs("outputs")
        # csv dosyası oluşturuldu
        df.to_csv("outputs/job_search_data.csv", index=False)
        print("Veri Bitti")
        return df



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
    response = requests.get(
        f'https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}',
        cookies=cookies,
        headers=headers,
    )

    response.raise_for_status()

    return response.json()["data"], response.json()["included"]
def get_job_detail_dataframe(job_id):

    job_json, included_json = fetch_job_details_json(job_id=job_id)

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

    job_dict = {"job_description":[job_description], "job_apply_count":[job_apply_count],
              "expire_date":[expire_date], "employment_status":[employment_status],
              "experience_level":[experience_level], "industries":[industries],
              "job_functions":[job_functions], "location":[location],
              "listed_date":[listed_date], "original_listed_date":[original_listed_date],
              "title":[title], "views":[views], "is_remote":[is_remote]}


    job_detail_dataframe = pd.DataFrame(job_dict)

    return job_detail_dataframe
def make_jobdetails_list(DataFrame):

    counter = 0
# DataFrame'in satırlarında dolaşmak için tqdm kullanımı
    for _, row in tqdm(DataFrame.iterrows(), total=len(DataFrame)):
        job_id = row["job_id"]

        try:
            dataframe = get_job_detail_dataframe(job_id)
        except:
            print("Error:", job_id)
            time.sleep(10)
        job_detail_df = pd.concat([job_detail_df, dataframe], ignore_index=True)

        time.sleep(random.randint(0, 1))

        counter += 1

        # Her 50 işlemde Excel dosyasına kayıt oluştur.
        # Oluşturamazsan 10 saniye dur.
        if counter == 50:
            try:
                job_detail_df.to_csv("jobdetail.csv")
                print("Excel dosyası oluşturuldu")
            except Exception as e:
                print(e, response.status_code)
                time.sleep(10)
            counter = 0
    return job_detail_df
########################################




    if __name__ == "__main__":
        cookies = cookies
        headers = headers

