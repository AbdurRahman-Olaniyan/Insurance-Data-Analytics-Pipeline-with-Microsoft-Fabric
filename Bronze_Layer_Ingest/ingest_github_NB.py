#!/usr/bin/env python
# coding: utf-8

# ## ingest_github_NB
# 
# New notebook

# In[1]:


#Ingest source data from github


# In[1]:


import requests

OWNER = "AbdurRahman-Olaniyan"
REPO = "Insurance_data"
BRANCH = "main"
FOLDER_PATH = ""

BRONZE_DIR = "Files/bronze/github"


# In[2]:


def get_csv_file_list():

    api_url = (
        f"https://api.github.com/repos/"
        f"{OWNER}/{REPO}/contents/{FOLDER_PATH}?ref={BRANCH}"
    )

    response = requests.get(api_url)
    response.raise_for_status()

    repo_items = response.json()

    csv_files = [
        item for item in repo_items
        if item["type"] == "file"
        and item["name"].lower().endswith(".csv")
    ]

    return csv_files


# In[3]:


def copy_file_to_bronze(download_url, filename):

    print("Downloading:", filename)

    response = requests.get(download_url)
    response.raise_for_status()

    tmp_path = f"/tmp/{filename}"

    with open(tmp_path, "wb") as f:
        f.write(response.content)

    try:
        fs = notebookutils.fs
    except:
        fs = mssparkutils.fs

    fs.mkdirs(BRONZE_DIR)

    dest_path = f"{BRONZE_DIR}/{filename}"
    fs.cp(f"file:{tmp_path}", dest_path, True)

    print("Saved:", dest_path)


# In[4]:


def run_github_bronze_ingestion():

    files = get_csv_file_list()

    print("CSV files found:")
    for f in files:
        print("-", f["name"])

    print("\nStarting copy...\n")

    for f in files:
        copy_file_to_bronze(
            download_url=f["download_url"],
            filename=f["name"]
        )

    print("\nBronze ingestion complete")


# In[5]:


run_github_bronze_ingestion()

