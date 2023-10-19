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
    'UserMatchHistory': 'AQKECj9ChBB0WAAAAYtBb352ntprG4J4sjJzzHdVQRi23Dxj22QI9eeeBSZaFMOWWiBGY-C7HrsZGClO73ml3tctuhWCMZMli1cohRMHlGbBCkDGiR8Jek5qogJVBx1x4FegedwYzlvP80FT8KuiD2GuscBs3J1kabi5X95PjDrx_muFpa4rbw_FB7htRM2QqayMt6t6DvaWqFfkos_-mvQOC02v4jaUafhO6E4BUCy4tukle-M3EncUETRu8AkbW_5kh-a0kWyZINZF9gx6DLujsSC1RZfoBjgXZNxsaF1XMdXQnhw41GZoIisGg0WZLWJNYKKN1WntRoc3fTDvzm3OIgUhCXI',
    'sdsc': '22%3A1%2C1697609908741%7EJAPP%2C0aBATZiSEeAwqv9GAGKHA44hsZ50%3D',
    'lidc': '"b=VB99:s=V:r=V:a=V:p=V:g=2900:u=38:x=1:i=1697609908:t=1697669953:v=2:sig=AQFukoMhZy1Myt_LnipZzCC1Mh-abnEF"',
    'AMCV_14215E3D5995C57C0A495C55%40AdobeOrg': '-637568504%7CMCIDTS%7C19648%7CMCMID%7C84896306024057589406665569500087326612%7CMCOPTOUT-1697617110s%7CNONE%7CvVersion%7C5.1.1',
}

headers = {
    'accept': 'application/vnd.linkedin.normalized+json+2.1',
    'accept-language': 'tr-TR,tr;q=0.7',
    'csrf-token': 'ajax:0363227315382656329',
    'referer': 'https://www.linkedin.com/jobs/search/?currentJobId=3673664912&distance=25&geoId=102105699&keywords=data%20engineer&origin=JOBS_HOME_SEARCH_CARDS',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'x-li-track': '{"clientVersion":"1.13.4942","mpVersion":"1.13.4942","osName":"web","timezoneOffset":3,"timezone":"Europe/Istanbul","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":843,"displayHeight":945}',
    'x-restli-protocol-version': '2.0.0',
}
