from function.helper_functions import *
from config.conf import *


url = generate_url("Data Analyst", "İsviçre")

result_json = fetch_data(url)

df = job_list_to_csv("Data Analyst", "İsviçre")


url = generate_url("Data Engineer", "Amerika Birleşik Devletleri")

result_json = fetch_data(url)

usa_df = job_list_to_csv("Data Engineer", "Amerika Birleşik Devletleri")


url = generate_url("Data Scientist", "Kanada")

result_json = fetch_data(url)

can_df = job_list_to_csv("Data Scientist", "Kanada")

url = generate_url("Software Engineer", "İtalya")

result_json = fetch_data(url)

ita_df = job_list_to_csv("Software Engineer", "İtalya")


url = generate_url("Data Analyst", "Türkiye")
result_json = fetch_data(url)
tr_df, file_name = job_list_to_csv("Data Analyst", "Türkiye")

file_name

if __name__ == "__main__":
    generate_url()
    fetch_data(url)
    fetch_job_ids()
    job_list_to_csv()
    cookies
    headers
    logging.warning("Bu bir uyarı mesajıdır.")
    logging.error("Bu bir hata mesajıdır.")
    logging.critical("Bu bir kritik hata mesajıdır.")

