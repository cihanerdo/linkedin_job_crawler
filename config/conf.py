import requests
import os
from dotenv import load_dotenv, find_dotenv
from functions.logger import logger, console_handler
from sqlalchemy import create_engine
from http.cookiejar import MozillaCookieJar


load_dotenv(find_dotenv())

# get cookies
# https://chrome.google.com/webstore/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc/related
# use this extensions for getting cookies

cookies = MozillaCookieJar("config/cookie.txt")
cookies.load(ignore_discard=True, ignore_expires=True)

# fetch jsessionid for headers

for cookie in cookies:
    if cookie.name == "JSESSIONID":
        JSESSIONID = cookie.value.strip('"')
        break


headers = {
 'csrf-token': JSESSIONID,
 'accept': 'application/vnd.linkedin.normalized+json+2.1',
 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
}


# Database Information

DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST_IP = os.getenv("DB_HOST_IP")
DB_NAME = os.getenv("DB_NAME")



geoid_dict = {"turkey": "102105699", "germany": "101282230", "switzerland": "106693272", "usa": "103644278",
              "france": "105015875", "canada": "101174742", "denmark": "104514075", "united kingdom": "101165590",
              "norway": "103819153", "sweden": "105117694", "poland": "105072130", "netherlands": "102890719",
              "italy": "103350119", "japan": "101355337", "hungary": "100288700", "south africa": "104035573",
              "argentina": "100446943", "china": "102890883", "czechia": "104508036", "russia": "101728296",
              "spain": "105646813", "qatar": "104170880", "australia": "101452733", "united arab emirates ": "104305776",
              "india": "102713980", "saudi arabia": "100459316", "south korea": "105149562", "israel": "101620260",
              "lithuania": "101464403", "romania": "106670623", "crotia": "104688944", "bulgaria": "105333783",
              "serbia": "101855366", "greece": "104677530", "azerbaijan": "103226548", "ukraine": "102264497"}
