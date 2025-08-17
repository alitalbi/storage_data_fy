#Configuration for repo git connection
import os
import dotenv

dotenv.load_dotenv()
repo_name = "alitalbi/storage_data_fy"
local_repo_path = os.getcwd()
token = os.getenv("GIT_TOKEN")
branch = "master"
yahoo_interval = "1d"
xml_file_path = os.path.join(local_repo_path,"assets.xml")