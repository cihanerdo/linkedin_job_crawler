from functions.helper_functions import *
import argparse
from functions.logger import *


parser = argparse.ArgumentParser()
parser.add_argument("-j", "--job_title", default="data engineer", help="Enter the job title")
parser.add_argument("-l", "--location", default="Turkey", help="Enter the location")
parser.add_argument("-d", "--debug", action="store_true", help="Debug Mode")

# python main.py -j "job_title" -l "location" -d

def crawler():

    engine = create_postgresql_connection(DB_USERNAME, DB_PASSWORD, DB_HOST_IP, DB_NAME)
    args = parser.parse_args()
    
    is_debug = args.debug
    if is_debug:
        console_handler.setLevel(logging.DEBUG)
        logger.info("DEBUG mode activated.")

    job_title = args.job_title
    location = args.location.lower()

    job_ids_dataframe = fetch_job_ids(job_title, location, DEBUG=is_debug)

    try:
        job_ids_dataframe.to_sql(name="jobs", schema="stg", con=engine, index=False, if_exists='append')
        logger.info("Data has been successfully transferred to the database.")
    except Exception as e:
        raise logger.error("An error occurred while transfer jobs data")


    detailed_job_data = generate_job_details_csv(job_ids_dataframe, job_title, location, DEBUG=is_debug)

    try:
        detailed_job_data.to_sql(name="job_details", schema="stg",  con=engine, index=False, if_exists='append')
        logger.info("Data has been successfully transferred to the database.")
    except Exception as e:
        raise logger.error("An error occurred while transfer jobs data")



if __name__ == "__main__":

    crawler()
