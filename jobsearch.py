from function.helper_functions import *
from config.conf import *

cookies = {
    'bcookie': '"v=2&f2acaf57-4b21-4164-895c-40e116b6a390"',
    'bscookie': '"v=1&20230811194405aff7bc7b-a573-4c3b-8b05-e253d858c0e0AQFcAF1Q0YZzrcpQRzOCT_hiQMfEgmaK"',
    'JSESSIONID': '"ajax:0363227315382656329"',
    'timezone': 'Europe/Istanbul',
    'li_sugr': '67c70732-b0f2-48a9-8118-4c8c41e0b125',
    '_guid': 'bc4be384-adf5-456f-9a8d-fc4fe3116ddf',
    'li_theme': 'dark',
    'li_theme_set': 'user',
    '_gcl_au': '1.1.57884259.1694901924',
    'liap': 'true',
    'li_at': 'AQEDATJO64MASZTZAAABivasyXAAAAGLQTRs7k0A0RJX9dwrn8fmLxtZdYhIP46ZI6DSbRz3wWJyWxWY6sa6bsG_GE2M_4VSn6ll5BdLRW4hNlcE64oXYm-r8YZOlEhcW_uRGvp8aAWWLANKf-NnGuV6',
    'AnalyticsSyncHistory': 'AQJCz2Cd2F9uygAAAYsuGtGXyBqNipDKqBFHYjSpHAcCih4PWicp5ZJVLPt2O0lt9mPlfqHjm_OiYcbAPSPndA',
    'lms_ads': 'AQHosEo4AzQhYgAAAYsuGtLE79TZ_lWK4gySXof7S8p86CaoqvnGuJs_ekCx_ZYAF4vJIwHpVEM_YazMnNZdtSAwK-Fk4sCp',
    'lms_analytics': 'AQHosEo4AzQhYgAAAYsuGtLE79TZ_lWK4gySXof7S8p86CaoqvnGuJs_ekCx_ZYAF4vJIwHpVEM_YazMnNZdtSAwK-Fk4sCp',
    'lang': 'v=2&lang=tr-tr',
    'AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg': '1',
    'AMCV_14215E3D5995C57C0A495C55%40AdobeOrg': '-637568504%7CMCIDTS%7C19648%7CMCMID%7C84896306024057589406665569500087326612%7CMCOPTOUT-1697544737s%7CNONE%7CvVersion%7C5.1.1',
    'lidc': '"b=VB99:s=V:r=V:a=V:p=V:g=2898:u=38:x=1:i=1697537537:t=1697553476:v=2:sig=AQHUZvpwyyW9GUpdfDxkNQ-k8ZSnrzLs"',
    'sdsc': '1%3A1SZM1shxDNbLt36wZwCgPgvN58iw%3D',
    'UserMatchHistory': 'AQJOFFCKydRUUwAAAYs9NxEP6nDSBFBtWtrnWFZ3BN-rrzgJV6qAP5Lyej4S4RcDSpsRj7TGilRYhn5s00aZ6s3EgTUSVO211pPztLBiiOAkVcrOsOOxSKFpurvNNcZYgbCQqBnvTHJgBv8PpdQvWxKiNlTLdxHbspORVKcoiljZuoIjV2oAPiiWmqtSyu52NXaeZalgq39iaM5oty7BoG9y1ZKmdnMMvxviIfIdtOXM99j16cWYAfAHpf2UJFSOlM6VXyKFXzbVOPZh98tuSzYt4vjf2B7Gky1a4G5wYfm2oUSCMIOjmPW3nSlQgK63RJ7JV_GkvdNpC_ik2Zh_RN4meVpjEIo',
}

headers = {
    'authority': 'www.linkedin.com',
    'accept': 'application/vnd.linkedin.normalized+json+2.1',
    'accept-language': 'tr-TR,tr;q=0.6',
    'cache-control': 'no-cache',
    # 'cookie': 'bcookie="v=2&f2acaf57-4b21-4164-895c-40e116b6a390"; bscookie="v=1&20230811194405aff7bc7b-a573-4c3b-8b05-e253d858c0e0AQFcAF1Q0YZzrcpQRzOCT_hiQMfEgmaK"; JSESSIONID="ajax:0363227315382656329"; timezone=Europe/Istanbul; li_sugr=67c70732-b0f2-48a9-8118-4c8c41e0b125; _guid=bc4be384-adf5-456f-9a8d-fc4fe3116ddf; li_theme=dark; li_theme_set=user; _gcl_au=1.1.57884259.1694901924; liap=true; li_at=AQEDATJO64MASZTZAAABivasyXAAAAGLQTRs7k0A0RJX9dwrn8fmLxtZdYhIP46ZI6DSbRz3wWJyWxWY6sa6bsG_GE2M_4VSn6ll5BdLRW4hNlcE64oXYm-r8YZOlEhcW_uRGvp8aAWWLANKf-NnGuV6; AnalyticsSyncHistory=AQJCz2Cd2F9uygAAAYsuGtGXyBqNipDKqBFHYjSpHAcCih4PWicp5ZJVLPt2O0lt9mPlfqHjm_OiYcbAPSPndA; lms_ads=AQHosEo4AzQhYgAAAYsuGtLE79TZ_lWK4gySXof7S8p86CaoqvnGuJs_ekCx_ZYAF4vJIwHpVEM_YazMnNZdtSAwK-Fk4sCp; lms_analytics=AQHosEo4AzQhYgAAAYsuGtLE79TZ_lWK4gySXof7S8p86CaoqvnGuJs_ekCx_ZYAF4vJIwHpVEM_YazMnNZdtSAwK-Fk4sCp; lang=v=2&lang=tr-tr; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19648%7CMCMID%7C84896306024057589406665569500087326612%7CMCOPTOUT-1697544737s%7CNONE%7CvVersion%7C5.1.1; lidc="b=VB99:s=V:r=V:a=V:p=V:g=2898:u=38:x=1:i=1697537537:t=1697553476:v=2:sig=AQHUZvpwyyW9GUpdfDxkNQ-k8ZSnrzLs"; sdsc=1%3A1SZM1shxDNbLt36wZwCgPgvN58iw%3D; UserMatchHistory=AQJOFFCKydRUUwAAAYs9NxEP6nDSBFBtWtrnWFZ3BN-rrzgJV6qAP5Lyej4S4RcDSpsRj7TGilRYhn5s00aZ6s3EgTUSVO211pPztLBiiOAkVcrOsOOxSKFpurvNNcZYgbCQqBnvTHJgBv8PpdQvWxKiNlTLdxHbspORVKcoiljZuoIjV2oAPiiWmqtSyu52NXaeZalgq39iaM5oty7BoG9y1ZKmdnMMvxviIfIdtOXM99j16cWYAfAHpf2UJFSOlM6VXyKFXzbVOPZh98tuSzYt4vjf2B7Gky1a4G5wYfm2oUSCMIOjmPW3nSlQgK63RJ7JV_GkvdNpC_ik2Zh_RN4meVpjEIo',
    'csrf-token': 'ajax:0363227315382656329',
    'pragma': 'no-cache',
    'referer': 'https://www.linkedin.com/jobs/search/?currentJobId=3718945267&geoId=102105699&keywords=data%20engineer&location=T%C3%BCrkiye&origin=JOB_SEARCH_PAGE_SEARCH_BUTTON&refresh=true',
    'sec-ch-ua': '"Chromium";v="118", "Brave";v="118", "Not=A?Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sec-gpc': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'x-li-deco-include-micro-schema': 'true',
    'x-li-lang': 'tr_TR',
    'x-li-page-instance': 'urn:li:page:d_flagship3_search_srp_jobs;QxX5k787SnuCDDznDQ4vJw==',
    'x-li-pem-metadata': 'Voyager - Careers=jobs-search-results',
    'x-li-track': '{"clientVersion":"1.13.4942","mpVersion":"1.13.4942","osName":"web","timezoneOffset":3,"timezone":"Europe/Istanbul","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":863,"displayHeight":949}',
    'x-restli-protocol-version': '2.0.0',
}

response = requests.get(
    'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-186&count=25&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_SEARCH_BUTTON,keywords:data%20engineer,locationUnion:(geoId:102105699),spellCorrectionEnabled:true)&start=0',
    cookies=cookies,
    headers=headers,
)
veri = 0
start_count = 0
url = f'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-186&count=25&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_SEARCH_BUTTON,keywords:data%20engineer,locationUnion:(geoId:102105699),spellCorrectionEnabled:true)&start={start_count}'

result_json = helper_functions.fetch_data(url)

helper_functions.job_make_work_list()

if __name__ == "__main__":
    fetch_data(url,cookies=cookies, headers=headers)
    fetch_job_ids()
    job_make_work_list()
    cookies
    headers
    response
