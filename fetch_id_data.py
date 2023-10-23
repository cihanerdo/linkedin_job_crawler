from function.helper_functions import *
from config.conf import *





url = generate_url("Data Engineer")

de = fetch_data(url)

json_to_dataframe_job_ids(de)

df = fetch_job_ids("Data Analyst", "Türkiye")




