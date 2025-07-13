#Configuration for repo git connection
import os
import dotenv

dotenv.load_dotenv()
cwd = os.getcwd()
repo_name = "alitalbi/storage_data_fy"
local_repo_path = "C:Users\Administrateur\storage_data_fy"
token = os.getenv("GIT_TOKEN")
branch = "master"
yahoo_interval = "30m"
xml_file_path = os.path.join(cwd,"assets.xml")