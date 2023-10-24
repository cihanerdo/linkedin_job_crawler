from function.helper_functions import *
from config.conf import *


url = generate_url("data analyst", "germany")
result_json = fetch_data(url)
json_to_dataframe_job_ids(result_json)
df = fetch_job_ids("data analyst", "germany", DEBUG=False)



if __name__ == "__main__":
    url = generate_url("data analyst", "germany")
    result_json = fetch_data(url)
    json_to_dataframe_job_ids(result_json)
    df = fetch_job_ids("data analyst", "germany", DEBUG=False)