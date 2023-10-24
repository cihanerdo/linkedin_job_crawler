import pandas as pd

from function.helper_functions import *

sample_json = fetch_job_details_json(job_id=3720343890)
sample_job_detail = get_job_detail_dataframe(3720343890)
sample_all_job_detail = generate_job_details_csv("Data Analyst", "germany", DEBUG=False)


if __name__ == "__main__":
    fetch_job_details_json(job_id=3731588298)
    get_job_detail_dataframe(3731588298)
    generate_job_details_csv("Data Analyst", "germany")
