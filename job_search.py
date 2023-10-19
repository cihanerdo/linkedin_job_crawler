from function.helper_functions import *
from config.conf import *

url = generate_url("Data Analyst", "İsviçre")

result_json = fetch_data(url)

df = job_list_to_csv("Data Analyst", "İsviçre")


url = generate_url("Data Engineer", "Amerika Birleşik Devletleri")

result_json = fetch_data(url)

usa_df = job_list_to_csv("Data Engineer", "Amerika Birleşik Devletleri")



if __name__ == "__main__":
    fetch_data(url,cookies=cookies, headers=headers)
    fetch_job_ids()
    job_make_work_list()
    cookies
    headers
    response
    start_count