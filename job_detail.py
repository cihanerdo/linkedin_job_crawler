import pandas as pd

from function.helper_functions import *


fetch_job_details_json(job_id=3729317819)
job_info = get_job_detail_dataframe(3729317819)

job_details_to_csv(usa_df)

fetch_job_details_json(job_id=3742881610)
job_info = get_job_detail_dataframe(3742881610)

job_details_to_csv(can_df)

fetch_job_details_json(job_id=3728429475)
job_info = get_job_detail_dataframe(3728429475)

job_details_to_csv(ita_df)

fetch_job_details_json(job_id=3735697980)
job_info = get_job_detail_dataframe(3735697980)
job_details_to_csv(tr_df)


job_ids = pd.read_csv("outputs/job_search_data.csv")

fetch_job_details_json(job_id=3402129076)
job_info = get_job_detail_dataframe(3402129076)
job_details_to_csv(job_ids, DEBUG=True)


fetch_job_details_json(job_id=3723247395)
get_job_detail_dataframe(3723247395)
job_details_to_csv(df)

if __name__ == "__main__":
    cookies
    headers
    fetch_job_details_json(cookies=cookies, headers=headers, job_id=3731588298)
    get_job_detail_dataframe()
    job_details_to_csv()
