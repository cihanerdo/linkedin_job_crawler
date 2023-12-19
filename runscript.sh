#!/bin/bash

job_title="$1"
location="$2"

# env activation
source /home/cihanerdogan64/miniconda3/bin/activate linkedin-crawler

cd /home/cihanerdogan64/projects/linkedin_job_crawler

# run script 
python3 main.py -j "$job_title" -l "$location" 

