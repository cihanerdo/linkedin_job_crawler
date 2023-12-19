from functions.helper_functions import *

if __name__ == "__main__":
    sample_json = fetch_job_details_json(job_id=3720343890)
    sample_job_detail = get_job_detail_dataframe(3720343890)
    sample_all_job_detail = generate_job_details_csv("Data Analyst", "germany", DEBUG=False)
