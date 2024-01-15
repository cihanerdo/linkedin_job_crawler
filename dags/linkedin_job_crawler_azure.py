from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from functions.helper_functions import *
from itertools import product
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from azure.storage.blob import BlobServiceClient, ContentSettings
import os 


AZURE_CONNECTION_STRING = Variable.get("azure_conn", default_var=None)

BLOB_CONTAINER_NAME = "cihan"


JOB_IDS_LOCAL_DIR = "dags/outputs/job_ids"
JOB_DETAILS_LOCAL_DIR = "dags/outputs/job_details"

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
    import nltk
    nltk.download('stopwords')

    location = location.lower()
    logger.debug('job_details_to_csv function started.')

    all_job_details_df = pd.DataFrame()
    counter = 0

    
    today = datetime.today().strftime("%d-%m-%y")
    job_title_lower = job_title.lower()
    location_lower = location.lower()
    file_name = f'{job_title_lower}_{location_lower}_job_ids_data_{today}'
    file_path = f"dags/outputs/job_ids/{file_name}.csv"
    job_ids_dataframe = pd.read_csv(file_path)

    if DEBUG:
        job_ids_dataframe = pd.read_csv(file_path)
        job_ids_dataframe = job_ids_dataframe.head(5)

    
    for _, row in tqdm(job_ids_dataframe.iterrows(), total=len(job_ids_dataframe)):
        job_id = row["job_id"]

        try:
            job_detail_df = get_job_detail_dataframe(job_id)
            logger.debug(f'Job ID: {job_id}, job_detail_df completed successfully.')

        except Exception as e:
            logger.error(f'Hata oluştu: Job ID - {job_id}, Hata: {str(e)}')
            time.sleep(10)

        all_job_details_df = pd.concat([all_job_details_df, job_detail_df], ignore_index=True)

        time.sleep(random.randint(0, 1))

        counter += 1

        
        if counter == 25 or counter == len(job_ids_dataframe):
            try:
                if len(all_job_details_df) == 250:
                    all_job_details_df = skills_catcher_db(all_job_details_df)
                    os.makedirs("dags/outputs/job_details/", exist_ok=True)
                    today = datetime.today().strftime("%d-%m-%y")

                    job_title = job_title.lower()
                    location = location.lower()
                    file_name = f'{job_title}_{location}_job_details_data_{today}'
                    file_path = f"dags/outputs/job_details/{file_name}.csv"
                    
                    all_job_details_df.to_csv(file_path, index=False)
                    logger.debug(f'CSV file creation completed successfully.: {file_name}')
                
                
                
                else:
                
                    all_job_details_df = skills_catcher(all_job_details_df)

                    os.makedirs("dags/outputs/job_details/", exist_ok=True)
                    today = datetime.today().strftime("%d-%m-%y")

                    job_title = job_title.lower()
                    location = location.lower()
                    file_name = f'{job_title}_{location}_job_details_data_{today}'
                    file_path = f"dags/outputs/job_details/{file_name}.csv"
                    
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
        os.makedirs("dags/outputs/job_ids/", exist_ok=True)

        # Dosya Yolu Oluştur ve CSV Dosyasını Kaydet
        job_title = job_title.lower()
        location = location.lower()

        today = datetime.today().strftime("%d-%m-%y")
        file_name = f'{job_title}_{location}_job_ids_data_{today}'
        file_path = f"dags/outputs/job_ids/{file_name}.csv"


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
        os.makedirs("dags/outputs/job_ids", exist_ok=True)

        # Dosya Yolu Oluştur ve CSV Dosyasını Kaydet
        job_title = job_title.lower()
        location = location.lower()

        today = datetime.today().strftime("%d-%m-%y")
        file_name = f'{job_title}_{location}_job_ids_data_{today}'
        file_path = f"dags/outputs/job_ids/{file_name}.csv"

        all_job_ids_dataframe.to_csv(file_path, index=False)
        crawled_job_id_count = len(all_job_ids_dataframe)
        total_job_count = result_json["data"]["paging"]["total"]
        logger.info(f"{crawled_job_id_count} of {total_job_count} crawled.")

        return file_path

    except Exception as e:
        # Hata günlüğüne hata mesajı ve ayrıntıları ekle
        logger.error("An error occurred while fetch_job_ids", str(e))

def upload_to_azure_blob(local_dir, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

    for filename in os.listdir(local_dir):
        blob_client = container_client.get_blob_client(blob_name + '/' + filename)
        with open(os.path.join(local_dir, filename), "rb") as data:
            blob_client.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type='text/csv'))

def check_csv_files_existence(data_folder, **kwargs):
    try:
        job_ids_folder = os.path.join(data_folder, 'job_ids')
        job_details_folder = os.path.join(data_folder, 'job_details')

        job_ids_files = [file for file in os.listdir(job_ids_folder) if file.endswith('.csv')]
        job_details_files = [file for file in os.listdir(job_details_folder) if file.endswith('.csv')]

        print("Job IDs Files:", job_ids_files)
        print("Job Details Files:", job_details_files)

        if job_ids_files:
            print("Returning tasks for upload.")
            return ['upload_job_ids_to_blob']
        else:
            return ['upload_job_details_to_blob']
        
    except Exception as e:
        print(f"An error occurred while checking CSV files existence: {str(e)}")
        return ['no_op']

def delete_csv_files(data_folder, **kwargs):
    try:
        subfolders = [subfolder for subfolder in os.listdir(data_folder) if os.path.isdir(os.path.join(data_folder, subfolder))]

        for subfolder in subfolders:
            folder_path = os.path.join(data_folder, subfolder)
            csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

            for csv_file in csv_files:
                file_path = os.path.join(folder_path, csv_file)
                os.remove(file_path)
                print(f"Deleted file: {file_path}")

    except Exception as e:
        print(f"An error occurred while deleting CSV files: {str(e)}")

def upload_csv_to_postgres(csv_file_path, table_name,  **kwargs):
    DB_USERNAME = Variable.get("DB_USERNAME", default_var=None)
    DB_PASSWORD = Variable.get("DB_PASSWORD", default_var=None)
    DB_HOST_IP = Variable.get("DB_HOST_IP", default_var=None)
    DB_NAME = Variable.get("DB_NAME", default_var=None)
    engine = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST_IP}:5432/{DB_NAME}"
    df = pd.read_csv(csv_file_path)
    df.to_sql(name=table_name, schema="stg" ,con=engine, if_exists='append', index=False)




with DAG(
    default_args=default_args,
    start_date=datetime(2024, 1, 15),
    catchup=False,
    dag_id="linkedin_job_crawler_azure",
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


    dum_task = DummyOperator(task_id="dum_task")

    with TaskGroup("csv_to_sql_job_ids_task", dag=dag) as csv_to_sql_job_ids_task:

        pre_task = dum_task 

        for job_title, location in product(job_titles, locations.keys()):

            csv_to_sql_job_ids_task_id = f"csv_to_sql_job_ids_task_{job_title.replace(' ', '_')}_{location.replace(' ', '_')}"
            csv_to_sql_job_ids_task = PythonOperator(
                task_id=csv_to_sql_job_ids_task_id,
                python_callable=upload_csv_to_postgres,
                op_kwargs={
                    "csv_file_path":f'dags/outputs/job_ids/{job_title.lower()}_{location.lower()}_job_ids_data_{datetime.today().strftime("%d-%m-%y")}.csv',
                    "table_name":"job_ids_airflow",
                    "conn_id":"postgres_conn"
                }
            )
            csv_to_sql_job_ids_task.set_upstream(pre_task)
            pre_task = csv_to_sql_job_ids_task



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

    pre_task = DummyOperator(task_id="pre_task")

    with TaskGroup("csv_to_sql_job_details_task", dag=dag) as csv_to_sql_job_details_task:

        previo_task = pre_task 

        for job_title, location in product(job_titles, locations.keys()):

            csv_to_sql_job_details_task_id = f"csv_to_sql_job_details_task_{job_title.replace(' ', '_')}_{location.replace(' ', '_')}"
            csv_to_sql_job_details_task = PythonOperator(
                task_id=csv_to_sql_job_details_task_id,
                python_callable=upload_csv_to_postgres,
                op_kwargs={
                    "csv_file_path":f'dags/outputs/job_details/{job_title.lower()}_{location.lower()}_job_details_data_{datetime.today().strftime("%d-%m-%y")}.csv',
                    "table_name": "job_details_airflow"
                }
            )
            csv_to_sql_job_details_task.set_upstream(previo_task)
            previo_task = csv_to_sql_job_details_task


    check_csv_files_task = BranchPythonOperator(
        task_id="check_csv_files_task",
        python_callable=check_csv_files_existence,
        op_args=['dags/outputs/'],
        provide_context=True,
    )

    upload_job_ids_task = PythonOperator(
        task_id='upload_job_ids_to_blob',
        python_callable=upload_to_azure_blob,
        op_kwargs={'local_dir': JOB_IDS_LOCAL_DIR, 'blob_name': 'job_ids'},
        dag=dag,
    )

# Task to upload job_details CSV files to Azure Blob Storage
    upload_job_details_task = PythonOperator(
        task_id='upload_job_details_to_blob',
        python_callable=upload_to_azure_blob,
        op_kwargs={'local_dir': JOB_DETAILS_LOCAL_DIR, 'blob_name': 'job_details'},
        dag=dag,
)

    delete_csv_files_task = PythonOperator(
        task_id="delete_csv_files_task",
        python_callable=delete_csv_files,
        op_args=['dags/outputs/'],
        provide_context=True,
    )


    create_dwh_tables_task = PostgresOperator(
        task_id = "create_dwh_tables_task",
        postgres_conn_id = "postgres_conn",
        sql = """
        CREATE TABLE IF NOT EXISTS dwh.job_title(
            job_title_id serial PRIMARY KEY,
            job_title varchar);

        CREATE TABLE IF NOT EXISTS dwh.company(
            company_id serial PRIMARY KEY,
            company_name varchar);

        CREATE TABLE IF NOT EXISTS dwh.country(
            country_id serial PRIMARY KEY,
            country varchar);

        CREATE TABLE IF NOT EXISTS dwh.location(
            location_id serial PRIMARY KEY,
            country_id INT REFERENCES dwh.country(country_id),
            job_location varchar);

        CREATE TABLE IF NOT EXISTS dwh.employment_status(
            employment_status_id serial PRIMARY KEY,
            employment_status varchar);

        CREATE TABLE IF NOT EXISTS dwh.experience_level(
            experience_level_id serial PRIMARY KEY,
            experience_level varchar);

        CREATE TABLE IF NOT EXISTS dwh.industries(
            industries_id serial PRIMARY KEY,
            industries varchar);

        CREATE TABLE IF NOT EXISTS dwh.skills(
            skills_id serial PRIMARY KEY,
            skills varchar);

        CREATE TABLE IF NOT EXISTS dwh.job_functions(
            job_functions_id serial PRIMARY KEY,
            job_functions varchar);

        CREATE TABLE IF NOT EXISTS dwh.jobs(
            job_id bigint PRIMARY KEY,
            job_title_id int REFERENCES dwh.job_title(job_title_id),
            company_id int REFERENCES dwh.company(company_id),
            country_id int REFERENCES dwh.country(country_id),
            location_id int REFERENCES dwh.location(location_id),
            employment_status_id int REFERENCES dwh.employment_status(employment_status_id),
            experience_level_id int REFERENCES dwh.experience_level(experience_level_id),
            industries_id int REFERENCES dwh.industries(industries_id),
            skills_id int REFERENCES dwh.skills(skills_id),
            job_functions_id int REFERENCES dwh.job_functions(job_functions_id),
            listed_date timestamp,
            load_date timestamp,
            job_apply_count int,
            VIEWS int,
            is_remote bool,
            job_description TEXT);

        CREATE TABLE IF NOT EXISTS dwh.jobs_(
            job_id bigint PRIMARY KEY,
            job_title varchar,
            job_search varchar,
            company_name varchar,
            country varchar,
            location varchar,
            employment_status varchar,
            experience_level varchar,
            industries varchar,
            job_functions varchar,
            listed_date timestamp,
            load_date timestamp,
            job_apply_count int,
            views int,
            is_remote bool,
            skills varchar,
            job_description TEXT);
        
        """
    )

    insert_values_task = PostgresOperator(
        task_id = "insert_values_task",
        postgres_conn_id = "postgres_conn",
        sql = """
            INSERT
        INTO
        dwh.job_title(job_title) 
    SELECT
        DISTINCT job_title
    FROM
        stg.job_ids_airflow;

            INSERT
        INTO
        dwh.company(company_name)
    SELECT
        DISTINCT company_name
    FROM
        stg.job_ids_airflow;

            INSERT
        INTO
        dwh.country(country)
    SELECT
        DISTINCT location_search_term
    FROM
        stg.job_ids_airflow;

            INSERT
        INTO
        dwh.location(job_location,
        country_id)
    SELECT
        DISTINCT j.LOCATION,
        c.country_id
    FROM
        stg.job_ids_airflow j
    INNER JOIN dwh.country c ON
        c.country = j.location_search_term;

            INSERT
        INTO
        dwh.employment_status(employment_status)
    SELECT
        DISTINCT employment_status
    FROM
        stg.job_details_airflow;

            INSERT
        INTO
        dwh.experience_level(experience_level)
    SELECT
        DISTINCT experience_level
    FROM
        stg.job_details_airflow;

            INSERT
        INTO
        dwh.industries(industries)
    SELECT
        DISTINCT industries
    FROM
        stg.job_details_airflow;

            INSERT
        INTO
        dwh.skills(skills)
    SELECT
        DISTINCT skills
    FROM
        stg.job_details_airflow;

            INSERT
        INTO
        dwh.job_functions(job_functions)
    SELECT
        DISTINCT job_functions
    FROM
        stg.job_details_airflow;

            INSERT INTO dwh.jobs_(job_id, job_title, job_search, company_name, 
                        country, location, employment_status, 
                        experience_level, industries, job_functions,
                        listed_date, load_date, job_apply_count,
                        views, is_remote, skills, job_description)
    WITH RankedJobs AS (
        SELECT
            j.job_id,
            j.job_title,
            j.job_title_search_term,
            j.company_name,
            j.location_search_term,
            j.location,
            jd.employment_status,
            jd.experience_level,
            jd.industries,
            jd.job_functions,
            jd.expire_date,
            j.load_date,
            jd.job_apply_count,
            jd.views,
            jd.is_remote,
            jd.skills,
            jd.job_description,
            ROW_NUMBER() OVER (PARTITION BY j.job_id ORDER BY jd.views DESC, jd.job_apply_count DESC)
        FROM 
            stg.job_ids_airflow j
        INNER JOIN
            stg.job_details_airflow jd ON j.job_id=jd.job_id
        
    )
    SELECT 
        cast(j.job_id AS bigint),
        cast(j.job_title AS varchar),
        cast(j.job_title_search_term AS varchar),
        cast(j.company_name AS varchar),
        cast(j.location_search_term AS varchar),
        cast(j.location AS varchar),
        cast(jd.employment_status AS varchar),
        cast(jd.experience_level AS varchar),
        cast(jd.industries AS varchar),
        cast(jd.job_functions AS varchar),
        cast(jd.expire_date AS timestamp),
        cast(j.load_date AS timestamp),
        cast(jd.job_apply_count AS int),
        cast(jd.views AS int),
        cast(jd.is_remote AS bool),
        cast(jd.skills AS varchar),
        jd.job_description 
    FROM
        RankedJobs j
    INNER JOIN
        stg.job_details_airflow jd ON jd.job_id = j.job_id
    WHERE
        j.row_number = 1
    ON CONFLICT (job_id) DO NOTHING;

    INSERT INTO dwh.split_skills(
        SELECT
            job_id,
            job_title,
            job_search,
            company_name,
            country,
            LOCATION,
            employment_status,
            experience_level,
            industries,
            job_functions,
            listed_date,
            load_date,
            job_apply_count,
            VIEWS,
            is_remote,
            job_description,
            TRIM(UNNEST(string_to_array(skills, ','))) AS skill
        FROM
            dwh.jobs_
    )
    ON CONFLICT (job_id, skill) DO NOTHING;

    INSERT
	INTO
	dwh.verified_jobs(
        WITH verification_cte AS (
	SELECT
		CASE
			WHEN job_search = 'Data Engineer'
			AND job_description ILIKE '%data engineer%' THEN 1
			WHEN job_search = 'Data Analyst'
			AND job_description ILIKE '%data analyst%' THEN 1
			WHEN job_search = 'Data Scientist'
			AND job_description ILIKE '%data scientist%' THEN 1
			ELSE 0
		END AS is_verified,
		*
	FROM
		dwh.split_skills
        )
	SELECT
		*
	FROM
		verification_cte
	WHERE
		is_verified = 1
	)
	ON CONFLICT (job_id, skill) DO NOTHING;
                """
    )


    start_task >> fetch_job_data_task >> dum_task >> csv_to_sql_job_ids_task >> dummy_task >> generate_job_details_task >> pre_task
    pre_task >> csv_to_sql_job_details_task >> create_dwh_tables_task >> insert_values_task >>check_csv_files_task >> upload_job_ids_task >> upload_job_details_task 
    upload_job_details_task >> delete_csv_files_task