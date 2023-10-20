import requests

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
    'AnalyticsSyncHistory': 'AQLZofdEL4vHwgAAAYs9qwRopfVpq_PXvTUYwrRKf5xIM7VMVG8jJHgOstckGE7A7iZCCkB2-P3-lYvIDvacZQ',
    'lms_ads': 'AQEZylcbyjrm4wAAAYs9qwVE5Zi0L9lU7xzx_VOMMND1xmAP6I-daWY66qIrkNr-0BDq2Cx94Eij9nDGfoSnV6A_BTA3zbIB',
    'lms_analytics': 'AQEZylcbyjrm4wAAAYs9qwVE5Zi0L9lU7xzx_VOMMND1xmAP6I-daWY66qIrkNr-0BDq2Cx94Eij9nDGfoSnV6A_BTA3zbIB',
    'lang': 'v=2&lang=tr-tr',
    'li_at': 'AQEDATJO64MASZTZAAABivasyXAAAAGLZUSyZU0AFhLNruu9gDVnrHtYL7oTYBT2lY8pfmT1BAPwgeQyn74NlLu5ZeyHTrS8QIKJChS58J1VO2JJacr-k-_HqB8LeBGrNKTkeT13cyXdCm-UB0lCZCYF',
    'AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg': '1',
    'AMCV_14215E3D5995C57C0A495C55%40AdobeOrg': '-637568504%7CMCIDTS%7C19648%7CMCMID%7C84896306024057589406665569500087326612%7CMCOPTOUT-1697617110s%7CNONE%7CvVersion%7C5.1.1',
    'sdsc': '22%3A1%2C1697618783757%7EJAPP%2C0GMVVUEIeEetmOEAyBlZQqiZ6kzc%3D',
    'lidc': '"b=OB99:s=O:r=O:a=O:p=O:g=538:u=39:x=1:i=1697619853:t=1697621711:v=2:sig=AQHMIWDB_pRIs2hqSfuIouMMJKgmVbyK"',
    'UserMatchHistory': 'AQJKqx-X66d8-wAAAYtCB0K6lBCyFSVzyzAX57JNaWIk3ZqsqKfy8dfJESRbtKdCCtX6Nh-phxem3MQ53M7M_6ZOfRV4Hh-WrYZ0vY9QwUNdYXcNiIlxWl-FytjHsRlb2qCDa_r4bXz0z-Dv-ta1MOaA2i1vP-E6GwzBNXTz2U4DpkFxrVHRq61Ry7RdecoQAaTXJN1w0w1Zf5nWVR9XV1Z3oznxRwWGYoRtcOaxwmCMhhTJFqeBVcZEEFky8lVr24U2NzCbvm98F4v4nt2bivxNdd24v3Cpu9fNIBJG8HgyfsHO6pvP08yEnJ_V4Ascm86ZmrCIWdzE_53qxinStKqldM8rsF0',
}

headers = {
    'authority': 'www.linkedin.com',
    'accept': 'application/vnd.linkedin.normalized+json+2.1',
    'accept-language': 'tr-TR,tr;q=0.7',
    'cache-control': 'no-cache',
    # 'cookie': 'bcookie="v=2&f2acaf57-4b21-4164-895c-40e116b6a390"; bscookie="v=1&20230811194405aff7bc7b-a573-4c3b-8b05-e253d858c0e0AQFcAF1Q0YZzrcpQRzOCT_hiQMfEgmaK"; JSESSIONID="ajax:0363227315382656329"; timezone=Europe/Istanbul; li_sugr=67c70732-b0f2-48a9-8118-4c8c41e0b125; _guid=bc4be384-adf5-456f-9a8d-fc4fe3116ddf; li_theme=dark; li_theme_set=user; _gcl_au=1.1.57884259.1694901924; liap=true; AnalyticsSyncHistory=AQLZofdEL4vHwgAAAYs9qwRopfVpq_PXvTUYwrRKf5xIM7VMVG8jJHgOstckGE7A7iZCCkB2-P3-lYvIDvacZQ; lms_ads=AQEZylcbyjrm4wAAAYs9qwVE5Zi0L9lU7xzx_VOMMND1xmAP6I-daWY66qIrkNr-0BDq2Cx94Eij9nDGfoSnV6A_BTA3zbIB; lms_analytics=AQEZylcbyjrm4wAAAYs9qwVE5Zi0L9lU7xzx_VOMMND1xmAP6I-daWY66qIrkNr-0BDq2Cx94Eij9nDGfoSnV6A_BTA3zbIB; lang=v=2&lang=tr-tr; li_at=AQEDATJO64MASZTZAAABivasyXAAAAGLZUSyZU0AFhLNruu9gDVnrHtYL7oTYBT2lY8pfmT1BAPwgeQyn74NlLu5ZeyHTrS8QIKJChS58J1VO2JJacr-k-_HqB8LeBGrNKTkeT13cyXdCm-UB0lCZCYF; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C19648%7CMCMID%7C84896306024057589406665569500087326612%7CMCOPTOUT-1697617110s%7CNONE%7CvVersion%7C5.1.1; sdsc=22%3A1%2C1697618783757%7EJAPP%2C0GMVVUEIeEetmOEAyBlZQqiZ6kzc%3D; lidc="b=OB99:s=O:r=O:a=O:p=O:g=538:u=39:x=1:i=1697619853:t=1697621711:v=2:sig=AQHMIWDB_pRIs2hqSfuIouMMJKgmVbyK"; UserMatchHistory=AQJKqx-X66d8-wAAAYtCB0K6lBCyFSVzyzAX57JNaWIk3ZqsqKfy8dfJESRbtKdCCtX6Nh-phxem3MQ53M7M_6ZOfRV4Hh-WrYZ0vY9QwUNdYXcNiIlxWl-FytjHsRlb2qCDa_r4bXz0z-Dv-ta1MOaA2i1vP-E6GwzBNXTz2U4DpkFxrVHRq61Ry7RdecoQAaTXJN1w0w1Zf5nWVR9XV1Z3oznxRwWGYoRtcOaxwmCMhhTJFqeBVcZEEFky8lVr24U2NzCbvm98F4v4nt2bivxNdd24v3Cpu9fNIBJG8HgyfsHO6pvP08yEnJ_V4Ascm86ZmrCIWdzE_53qxinStKqldM8rsF0',
    'csrf-token': 'ajax:0363227315382656329',
    'pragma': 'no-cache',
    'referer': 'https://www.linkedin.com/jobs/search/?currentJobId=3740013817&distance=25&geoId=102105699&keywords=data%20engineer&origin=JOBS_HOME_SEARCH_CARDS',
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
    'x-li-page-instance': 'urn:li:page:d_flagship3_search_srp_jobs;LAWaQ3ueTW2PX+LLU1grDw==',
    'x-li-pem-metadata': 'Voyager - Careers=jobs-search-results',
    'x-li-track': '{"clientVersion":"1.13.4942","mpVersion":"1.13.4942","osName":"web","timezoneOffset":3,"timezone":"Europe/Istanbul","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":843,"displayHeight":945}',
    'x-restli-protocol-version': '2.0.0',
}

geoid_dict = {"Türkiye": "102105699", "Almanya": "101282230", "İsviçre": "106693272", "Amerika Birleşik Devletleri": "103644278",
"Fransa": "105015875", "Kanada": "101174742", "Danimarka": "104514075", "Birleşik Krallık": "101165590",
"Norveç": "103819153", "İsveç": "105117694", "Polonya": "105072130", "Hollanda": "102890719", "İtalya": "103350119",
"Japonya": "101355337", "Macaristan": "100288700", "Güney Afrika": "104035573", "Arjantin": "100446943",
"Çin": "102890883", "Çekya": "104508036", "Rusya": "101728296", "İspanya": "105646813", "Katar": "104170880",
"Avustralya": "101452733", "Birleşik Arap Emirlikleri": "104305776", "Hindistan": "102713980", "Suudi Arabistan": "100459316",
"Güney Kore": "105149562", "İsrail": "101620260", "Litvanya": "101464403", "Romanya": "106670623", "Hırvatistan": "104688944",
"Bulgaristan": "105333783", "Sırbistan": "101855366", "Yunanistan": "104677530", "Azerbaycan": "103226548", "Ukrayna": "102264497"}

job_title_dict = {
    "Data Analyst": "data%20analyst",
    "Data Engineer": "data%20engineer",
    "Data Scientist": "data%20scientist",
    "Data Science": "data%20science",
    "Software Engineer": "software%20engineer"
}

