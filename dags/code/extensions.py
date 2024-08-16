import os
from datetime import datetime
from github import Github
import os
from dotenv import load_dotenv, dotenv_values

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time 

load_dotenv()

'''
Database information
'''
DATABASE_NAME = "House_prices"
COLLECTION_NAME = "HCMCity"
MONGO_URI = os.getenv('MONGO_URI')

'''
Github information
'''
GITHUB_USERNAME = os.getenv('GITHUB_USERNAME')
TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('GITHUB_REPO_NAME')

today = datetime.now().date()

headers = {
  "Accept": "*/*",
  "Accept-Encoding": "gzip, deflate, br",
  "Accept-Language": "vi,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
  "Referer": "https://mogi.vn/",
  "Sec-Ch-Ua": "\"Microsoft Edge\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
  "Sec-Ch-Ua-Mobile": "?0",
  "Sec-Ch-Ua-Platform": "\"Windows\"",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-origin",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
}

def connect_github(username='TTAT91A', password=TOKEN, repo_name=REPO_NAME):
    g = Github(username, password)
    repo = g.get_user().get_repo(repo_name)
    return repo

def get_all_files(username='TTAT91A', password=TOKEN, repo_name=REPO_NAME):
    # repo = g.get_user().get_repo(REPO_NAME)
    repo = connect_github(username, password, repo_name=repo_name)

    all_files = []
    contents = repo.get_contents("")
    while contents:
        file_content = contents.pop(0)
        if file_content.type == "dir":
            contents.extend(repo.get_contents(file_content.path))
        else:
            file = file_content
            all_files.append(str(file).replace('ContentFile(path="','').replace('")',''))
    return all_files

def pushToGithub(local_file_path, file_name, git_path, username=GITHUB_USERNAME, password=TOKEN, repo_name=REPO_NAME):
    repo = connect_github(username, password, repo_name=repo_name)
    all_files = get_all_files(username, password, repo_name=repo_name)

    if os.path.exists(local_file_path):
        with open(local_file_path, 'r', encoding='utf-8') as file:
            content = file.read()
    else:
        print(f'{local_file_path} not found')
        return

    # Upload to github
    git_file = git_path + file_name #check file in repo
    if git_file in all_files:
        contents = repo.get_contents(git_file)
        commit = "Updated file " + str(today)
        repo.update_file(contents.path, commit, content, contents.sha, branch="main")
        print(git_file + ' UPDATED')
    else:
        commit = "Upload file " + str(today)
        repo.create_file(git_file, commit, content, branch="main")
        print(git_file + ' CREATED')

def connect_mongodb(database_name=DATABASE_NAME, collection_name = COLLECTION_NAME, mongo_uri=MONGO_URI):
    from pymongo import MongoClient
    client = MongoClient(mongo_uri)
    db = client[database_name]
    col = db[collection_name]
    return col