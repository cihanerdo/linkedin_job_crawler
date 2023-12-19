from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from functions.helper_functions import *
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from itertools import product
from airflow.utils.task_group import TaskGroup


default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}


job_titles = ["Data Analyst", "Data Engineer", "Data Scientist"]
locations = {
    "turkey": "102105699",
    "germany": "101282230",
    "usa": "103644278",
    "france": "105015875",
    "canada": "101174742",
    "united kingdom": "101165590",
}

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
        if len(all_job_details_df) == 100:

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
        else:
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
            if result_json is None:
                logger.error("fetch_data returned None. Stopping fetch_job_ids process.")
                return None
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

def upload_to_gcs(data_folder, **kwargs):
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
        return ['upload_to_gcs_task'] if csv_files else ['no_op']
    except Exception as e:
        print(f"An error occurred while checking CSV files existence: {str(e)}")
        return ['no_op']

def delete_csv_files(data_folder, **kwargs):
    try:
        csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]

        for csv_file in csv_files:
            file_path = os.path.join(data_folder, csv_file)
            os.remove(file_path)
            print(f"Deleted file: {file_path}")

    except Exception as e:
        print(f"An error occurred while deleting CSV files: {str(e)}")


with DAG(
    default_args=default_args,
    start_date=datetime(2023, 1, 18),
    catchup=False,
    dag_id="linkedin_job_crawler",
    schedule_interval="@daily",
) as dag:

    start_task = DummyOperator(task_id='start_task')

    with TaskGroup("fetch_job_data_task", dag=dag) as fetch_job_data_task:

        previous_task = start_task

        for job_title, location in product(job_titles, locations.keys()):
            fetch_job_ids_task_id = f"fetch_job_task_{job_title.replace(' ', '_')}_{location.replace(' ', '_')}"
            generate_job_details_csv_task_id = f"generate_job_details_csv_task_{job_title.replace(' ', '_')}_{location.replace(' ', '_')}"

            fetch_job_ids_task = PythonOperator(
                task_id=fetch_job_ids_task_id,
                python_callable=fetch_job_ids_airflow,
                op_kwargs={
                    'job_title': job_title,
                    'location': location,
                    'DEBUG': False,
                },
            )

            previous_task >> fetch_job_ids_task
            previous_task = fetch_job_ids_task

    dummy_task = DummyOperator(task_id='dummy_task')


    with TaskGroup("generate_job_details_task", dag=dag) as generate_job_details_task:

        previous_task = dummy_task

        for job_title, location in product(job_titles, locations.keys()):

            generate_job_details_csv_task_id = f"generate_job_details_csv_task_{job_title.replace(' ', '_')}_{location.replace(' ', '_')}"
            generate_job_details_csv_task = PythonOperator(
                task_id=generate_job_details_csv_task_id,
                python_callable=generate_job_details_csv_airflow,
                op_kwargs={
                    'job_title': job_title,
                    'location': location,
                    'DEBUG': False,
                },
            )

            generate_job_details_csv_task.set_upstream(previous_task)
            previous_task = generate_job_details_csv_task


    check_csv_files_task = BranchPythonOperator(
        task_id="check_csv_files_task",
        python_callable=check_csv_files_existence,
        op_args=['dags/outputs/'],
        provide_context=True,
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs_task',
        python_callable=upload_to_gcs,
        op_args=['dags/outputs/'],
        provide_context=True,
    )


    delete_csv_files_task = PythonOperator(
        task_id="delete_csv_files_task",
        python_callable=delete_csv_files,
        op_args=['dags/outputs/'],
        provide_context=True,
    )

    start_task >> fetch_job_data_task >> dummy_task >> generate_job_details_task >> check_csv_files_task
    check_csv_files_task >> upload_to_gcs_task >> delete_csv_files_task
