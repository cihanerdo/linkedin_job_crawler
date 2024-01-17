# LINKEDIN JOB CRAWLER

![Untitled](README/Untitled.gif)

## **Repository Cloning**
To clone the repository, use the following command:

`git clone https://github.com/cihanerdo/linkedin_job_crawler.git`

## Obtaining Cookies

1. **Sign in to LinkedIn:**
    - Visit linkedin.com and log in to your account.
2. **Download Cookies Extension:**
    - Download the "Get cookies.txt LOCALLY" extension.
3. **Save Cookie File:**
    - Save the downloaded cookie file to the **`configs`** folder, naming it **`cookie.txt`**.
![Untitled](README/Untitled.png)

## Database Connection

Enter the necessary database information in the .env file:

`DB_USERNAME="your_username"`

`DB_PASSWORD=your_password`

`DB_HOST_IP="your_ip"`

`DB_NAME="your_database_name"`

## Installing Requirements 

Install the required dependencies using the following command:

`pip install -r requirements.txt`

## Installing Stopwords

If you haven't used the nltk library and stopwords before, execute the following steps:

* `import nltk`
* `nltk.download("stopwords")`

## How to Use

**Full Run Mode**
To run in full mode, use:

`python main.py -j "job_title" -l "location"`

### **Debug Mode**
To run in debug mode, use:

 `python main.py -j "job_title" -l "location" -d`

## Supported Countries


The supported countries and their geoid configurations are specified in the conf.py file.

If you want to add a different country, you can add its name and geoid information to the geoid_dict section in the conf.py file.

To find the geoid information for the desired country, you can search for it on the [LinkedIn Jobs](https://www.linkedin.com/jobs/) tab.

**Sample URL:** `https://www.linkedin.com/jobs/search/ currentJobId=3797992840&**geoId=101282230**&location=Germany`

| Country | Geoid |
| --- | --- |
| Turkey | 102105699 |
| Germany | 101282230 |
| Switzerland | 106693272 |
| USA | 103644278 |
| France | 105015875 |
| Canada | 101174742 |
| Denmark | 104514075 |
| United Kingdom | 101165590 |
| ... | … |

## Airflow Setup

Follow the steps below for Airflow installation:

`mkdir -p ./dags ./logs ./plugins ./config`

`echo -e "AIRFLOW_UID=$(id -u)" > .env`

`docker compose up —build -d`

Access the Airflow UI at http://localhost:8080 after the installation is complete.

![Untitled](README/Untitled%201.png)



## Project Example

`python main.py -j “data analyst” -l “turkey” -d`

**Data Analyst jobs in Turkey.**

![Untitled](README/Untitled%202.png)

**Details of Data Analyst jobs in Turkey**

![Untitled](README/Untitled%203.png)



**General information about jobs** 

![Untitled](README/Untitled%204.png)

**Detailed table of jobs**

![Untitled](README/Untitled%205.png)

## Superset

****************Results of Data Engineer jobs****************

![Untitled](README/Untitled%206.png)

**************************************************************************************Results for Data Engineer jobs in United States**************************************************************************************

![Untitled](README/Untitled%207.png)

******************************************************************************Entry Level Data Scientist Jobs******************************************************************************

![Untitled](README/Untitled%208.png)
