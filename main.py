import logging
from function.helper_functions import *
from config.conf import *
import os
import argparse
from logger import logger, console_handler



parser = argparse.ArgumentParser()
parser.add_argument("-j", "--job_title", default="data engineer", help="Enter the job title")
parser.add_argument("-l", "--location", default="Turkey", help="Enter the location")
parser.add_argument("-d", "--debug", action="store_true", help="Debug Mode")


if __name__ == "__main__":
    args = parser.parse_args()
    
    is_debug = args.debug
    if is_debug:
         console_handler.setLevel(logging.DEBUG)
         logger.info("DEBUG mode activated.")

    job_title = args.job_title
    location = args.location.lower()
    job_details = generate_job_details_csv("data scientist", "SOUTH KOREA", DEBUG=is_debug)
