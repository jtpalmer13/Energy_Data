import json
import time
import os
import re
import requests
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Initialize logging
logging.basicConfig(filename='server.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


# Function to load JSON files
def load_json_file(file_path):
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"[Argus_API] {file_path} not found.")
        return None
    except json.JSONDecodeError:
        logging.error(f"[Argus_API] Failed to decode JSON from {file_path}.")
        return None


# Function to list files in a directory
def list_files_in_directory(directory_path):
    try:
        file_names = [os.path.splitext(filename)[0] for filename in os.listdir(directory_path) if
                      os.path.isfile(os.path.join(directory_path, filename))]
        return file_names
    except Exception as e:
        logging.error(f"[Argus_API] An error occurred while listing files: {e}")
        return []


# Function to initialize Selenium WebDriver
def initialize_webdriver():
    chrome_options = Options()
    chrome_options.add_experimental_option('prefs', {
        "download.default_directory": os.getcwd(),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    return webdriver.Chrome(options=chrome_options)


# Function to login to Argus Media
def login_to_argus(driver, credentials):
    try:
        driver.get("https://myaccount.argusmedia.com/login?ReturnUrl=https://direct.argusmedia.com")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "username"))).send_keys(
            credentials['username'])
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "password"))).send_keys(
            credentials['password'])
        WebDriverWait(driver, 10).until(EC.presence_of_element_located(
            (By.XPATH, "/html/body/app-root/app-public/div[2]/div/app-login/div/div/form/div[2]/div/button"))).click()
    except Exception as e:
        logging.error(f"[Argus_API] Failed to login: {e}")


# Function to fetch file IDs
def fetch_file_ids(session):
    try:
        url = 'https://direct.argusmedia.com/integration/dataanddownloads/allitems?cats=Electricity'
        response = session.get(url)
        if response.status_code == 200:
            page_source = response.text
            match = re.search(r'Argus\.Bootstrap\.dataAndDownloadsHits = (\[.*?\]);', page_source)
            if match:
                return [article['fileId'] for article in json.loads(match.group(1))]
            else:
                logging.error("[Argus_API] Could not find file IDs in the page source.")
        else:
            logging.error(f"[Argus_API] Failed to fetch the URL {url}. Status code: {response.status_code}")
    except Exception as e:
        logging.error(f"[Argus_API] An error occurred while fetching file IDs: {e}")
    return []


def fetch_hisorical_file_ids(session):
    file_IDs = []

    for page in range(1, 14):
        try:
            time.sleep(TIME_DELAY)
            url = f'https://direct.argusmedia.com/integration/dataanddownloads/dataanddownloadsresultsonlyjson?page={page}&cats=Electricity'
            response = session.get(url)

            page_source = response.text if response.status_code == 200 else None
            page_source = json.loads(page_source)

            data_and_downloads_hits = [article['fileId'] for article in page_source['articles']]

            if data_and_downloads_hits:
                file_IDs += data_and_downloads_hits
        except:
            return file_IDs
    return file_IDs


def download_file(session, fileID):
    url = f'https://direct.argusmedia.com/integration/dataanddownloads/downloadfile/{fileID}'
    time.sleep(TIME_DELAY)
    response = session.get(url)

    # Check if the download was successful
    if response.status_code == 200:
        # Save the file
        with open(os.path.join(os.getcwd(), f'argus_downloads/{fileID}.xlsx'), 'wb') as f:
            f.write(response.content)
        logging.info(f"[Argus_API] Saved fileID {fileID}")
    else:
        logging.error(f'[Argus_API]Failed to download the file. Status code: {response.status_code}')


def main():
    global TIME_DELAY
    # Check folders
    create_folder_if_not_exists('argus_reformated')
    create_folder_if_not_exists('argus_downloads')

    # Load configurations and credentials
    config = load_json_file("configs/config.json").get('argus_api', {})
    credentials = load_json_file("configs/creds.json")
    argus_files = load_json_file("configs/files.json")

    if not all([config, credentials, argus_files]):
        return

    TIME_DELAY = config.get('time_delay', 0)
    GET_HISTORICAL = config.get('get_historical', False)

    # Initialize web driver and login
    driver = initialize_webdriver()
    login_to_argus(driver, credentials)

    # Wait for session cookies
    if driver.get_cookies()[0].get('domain') != 'direct.argusmedia.com':
        time.sleep(5)
        logging.info('[Argus_API] Waiting for Argus cookies')

    # Initialize session
    selenium_cookies = driver.get_cookies()
    session = requests.Session()
    for cookie in selenium_cookies:
        session.cookies.set(cookie['name'], cookie['value'])

    # Fetch file IDs
    if GET_HISTORICAL:
        file_ids = set(fetch_hisorical_file_ids(session))
    else:
        file_ids = set(fetch_file_ids(session))

    # Identify existing file IDs
    current_fileIDs = set(list_files_in_directory('argus_downloads')).union(
        set(list_files_in_directory('argus_reformated'))).union(
        set(argus_files.get('argus_files')))

    # Identify new file IDs
    new_fileIDs = file_ids - current_fileIDs

    logging.info(f"[Argus_API] Argus Files: {len(file_ids)}")
    logging.info(f"[Argus_API] Saved Files: {len(current_fileIDs)}")
    logging.info(f"[Argus_API] New Files: {len(new_fileIDs)}")

    for fileID in list(new_fileIDs):
        download_file(session, fileID)


if __name__ == "__main__":
    main()
