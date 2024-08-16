from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore

import urllib.parse
import pandas as pd
import numpy as np
# from selenium_provider import Selenium
# from selenium_provider import get_into
import requests
from bs4 import BeautifulSoup
from datetime import date
import os
from pathlib import Path

from github import Github
import os
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('GITHUB_TOKEN')
today = datetime.now().date()

# def connect_github(username, password, repo_name='Mogi_Pipeline_Airflow'):
#     g = Github(password)
#     repo = g.get_repo(username + "/" + repo_name)
#     return repo

# def get_all_files(username='TTAT91A', password=TOKEN, repo_name='Mogi_Pipeline_Airflow'):
#     # repo = g.get_user().get_repo('Mogi_HousePrices_Pipeline')
#     repo = connect_github(username, password, repo_name=repo_name)

#     all_files = []
#     contents = repo.get_contents("")
#     while contents:
#         file_content = contents.pop(0)
#         if file_content.type == "dir":
#             contents.extend(repo.get_contents(file_content.path))
#         else:
#             file = file_content
#             all_files.append(str(file).replace('ContentFile(path="','').replace('")',''))
#     return all_files

# def pushToGithub(local_file_path, file_name, username='TTAT91A', password=TOKEN, repo_name='Mogi_Pipeline_Airflow'):
#     # g = Github(username, password)
#     # # repo = g.get_user().get_repo('Mogi_HousePrices_Pipeline')
#     # repo = g.get_user().get_repo('House_Prices_Pipeline')
#     repo = connect_github(username, password, repo_name=repo_name)
#     all_files = get_all_files(username, password, repo_name=repo_name)
#     if os.path.exists(local_file_path):
#         with open(local_file_path, 'r', encoding='utf-8') as file:
#             content = file.read()
#     else:
#         print(f'{local_file_path} not found')
#         return

#     # Upload to github
#     git_file = 'dags/data1/' + file_name #check file in repo
#     if git_file in all_files:
#         contents = repo.get_contents(git_file)
#         commit = "Updated file " + str(today)
#         repo.update_file(contents.path, commit, content, contents.sha, branch="main")
#         print(git_file + ' UPDATED')
#     else:
#         commit = "Upload file " + str(today)
#         repo.create_file(git_file, commit, content, branch="main")
#         print(git_file + ' CREATED')

##########################
default_args = {
    'owner': 'TuanNguyen',
    'start_date': datetime.now(),
    'email': ['anhtuan.ltqb@gmail.com'],
    'email_on_failure': ['anhtuan.ltqb@gmail.com'],
    'email_on_retry': ['anhtuan.ltqb@gmail.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG( 
    'Run_Mogi_Pipeline',
    description='Scrape house prices in HCM City from mogi.vn',
    default_args=default_args,
    schedule_interval="@daily",
)

install_library = BashOperator(
    task_id='install_library',
    bash_command='pip install PyGithub pymongo python-dotenv',
    dag=dag,
)

run_get_house_link = BashOperator(
    task_id='get_house_link',
    bash_command='python /opt/airflow/dags/code/getHouseLinks.py',
    dag=dag,
)

run_get_info_house = BashOperator(
    task_id='get_house_info',
    bash_command='python /opt/airflow/dags/code/getHouseInfos.py',
    dag=dag,
)

run_preprocessing = BashOperator(
    task_id='run_preprocessing',
    bash_command='python /opt/airflow/dags/code/preprocess.py',
    dag=dag,
)

run_overpass = BashOperator(
    task_id='run_overpass',
    bash_command='python /opt/airflow/dags/code/getAmenities.py',
    dag=dag,
)

# loading_mongodb = BashOperator(
#     task_id='loading_mongodb',
#     bash_command='python /opt/airflow/dags/code/loadingMongoDB.py',
#     dag=dag,
# )

install_library >> run_get_house_link >> run_get_info_house >> run_preprocessing >> run_overpass
