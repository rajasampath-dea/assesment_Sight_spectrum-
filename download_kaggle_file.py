from configparser import ConfigParser
import argparse
import os
import sys
import requests
import base64

CONF_FILE = "C:/conf/configuration.ini" # where I will store all my credientials and environment variables
#username = "rajasampath"
#api_token = "XXXXXXXXXXXXXX" #token alphanumerical number here in conf/configuration.ini file

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--url", help="url of https://www.kaggle.com/datasets/ashirwadsangwan/imdb-dataset", required=True)

args = parser.parse_args()
url = args.url
if not url:
    print("Error: --url is required and cannot be empty")
    sys.exit(100)
    

def main():
    try:
        conf_parse = ConfigParser()
        conf_parse.read(CONF_FILE)
        
        username = conf_parse.get("conf", "username")
        token = conf_parse.get("conf", "api_token_key")
        auth_string = f"{username}:{token}"
        encoded_creds = base64.b64encode(auth_string.encode()).decode()     #encode my kaggle username and api_token_key , it converts my creds into ASCII string (system HTTPS readable encoding)
        #url = "https://www.kaggle.com/api/v1/datasets/download/ashirwadsangwan/imdb-dataset"
        file_name = url.split("/")[-1]
        print(file_name)
        
        headers = {"Authorization": f"Basic {encoded_creds}"  # HTTPS header encryption
                                                    }
        print(f"Downloading dataset : {file_name}...")
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        
        if response.status_code == 200:
            with open(r"C:/temp/{file_name}.zip", "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):  #giving chunk size , to maintain downloading balance over network
                    f.write(chunk)
            print(f"Download complete: {file_name}.zip")
            
            extract_path = "C:/data/temp/imdb/"
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)

            print(f"Extraction complete: {extract_path}")

    
        os.remove(zip_path)
        print(f"Deleted zip file: {zip_path}")
        
    except requests.exceptions.Timeout:
        print("Request timed out")

    except requests.exceptions.RequestException as req_err:
        print(f"Request failed: {req_err}")

    except Exception as e:
        print(f"Unexpected error: {e}")  
        
if __name__ == "__main__":
    main()