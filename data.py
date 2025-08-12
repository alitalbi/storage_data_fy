# data.py
import os
import xml.etree.ElementTree as ET
import pandas as pd
import yfinance as yf
from github import Github
import base64
from io import StringIO
from config import repo_name, local_repo_path, token, branch, xml_file_path, yahoo_interval
from git import Repo  # Requires 'GitPython' package: pip install GitPython
from datetime import datetime

def git_connect(token, repo_name):
    """
    Connect to GitHub repository using token.
    Returns: PyGithub repo object or None if repo doesn't exist.
    """
    try:
        g = Github(token)
        repo = g.get_repo(repo_name)  # Get repo directly (no need for user.get_repo)
        return repo
    except Exception as e:
        print(f"Error connecting to repo {repo_name}: {e}")
        return None


def check_update():
    """
    Check if local repo is up-to-date with remote branch.
    Returns: Tuple (is_up_to_date: bool, local_commit: str, remote_commit: str).
    """
    try:
        # Initialize local repository
        repo = Repo() #no need of local path
        if repo.bare:
            print(f"Local repo at {local_repo_path} is bare or invalid.")
            return False, None, None

        # Fetch remote info
        origin = repo.remotes.origin
        origin.fetch()

        # Compare local and remote HEAD commits
        local_commit = repo.head.commit.hexsha
        remote_commit = origin.refs[branch].commit.hexsha

        is_up_to_date = local_commit == remote_commit

        return is_up_to_date, local_commit, remote_commit
    except Exception as e:
        print(f"Error checking repo update: {e}")
        return False, None, None


def read_csv_git(repo, file_path):
    """
    Read a CSV file from GitHub repository.
    Args:
        repo: PyGithub repo object.
        file_path: Path to CSV file in repo (e.g., 'GC=F.csv').
    Returns: pandas DataFrame or None if file doesn't exist or error occurs.
    """
    try:
        file_content = repo.get_contents(file_path, ref=branch)
        decoded_content = base64.b64decode(file_content.content)
        string_content = StringIO(decoded_content.decode("utf-8"))
        df = pd.read_csv(string_content)
        df.columns = ["Date","Close","High","Low","Open","Volume"]
        return df
    except Exception as e:
        print(f"Error reading {file_path} from GitHub: {e}")
        return None


def get_data(ticker,mode,start):
    """
    Download hourly data for a ticker from Yahoo Finance.
    Args:
        ticker: String, e.g., 'GC=F'.
    Returns: pandas DataFrame with hourly data or None if download fails.
    """
    try:
        if mode == "latest_check":
            data = yf.download(ticker, period="5d",interval=yahoo_interval, progress=False)
            if data.empty:
                print(f"No data retrieved for {ticker}")
                return None
            return data
        elif mode == "append":
            data = yf.download(ticker, start=start, interval=yahoo_interval, progress=False)
            if data.empty:
                print(f"No data retrieved for {ticker}")
                return None
            return data
        elif mode == "update":
            data = yf.download(ticker, interval=yahoo_interval, progress=False)
            if data.empty:
                print(f"No data retrieved for {ticker}")
                return None
            return data
    except Exception as e:
        print(f"Error downloading data for {ticker}: {e}")
        return None

def reformat_data_yf(data):

    data.reset_index(inplace=True)
    data.columns = ["Date","Close","High","Low","Open","Volume"]
    data["Date"] = data["Date"].apply(lambda x:datetime.strftime(x, "%Y-%m-%d"))

    return data
def process_assets():
    """
    Read XML, compare with GitHub CSV files, download missing/outdated data.
    Returns: List of tuples (ticker, DataFrame) for assets needing update.
    """
    # Read XML file
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
    except Exception as e:
        print(f"Error reading XML file {xml_file_path}: {e}")
        return []

    # Extract all tickers from XML
    tickers = []
    for asset_class in root.findall(".//AssetClass"):
        for asset in asset_class:
            ticker = asset.find("Ticker").text
            if ticker and ticker not in tickers:  # Avoid duplicates
                tickers.append(ticker)

    # Connect to GitHub
    repo = git_connect(token, repo_name)
    if not repo:
        return []

    # Get list of CSV files in repo root
    try:
        contents = repo.get_contents("", ref=branch)
        git_files = [f.name for f in contents if f.name.endswith(".csv")]
    except Exception as e:
        print(f"Error listing GitHub files: {e}")
        return []

    updates_needed = []
    for ticker in tickers:
        csv_file = f"{ticker}.csv"
        latest_data = get_data(ticker, mode="latest_check", start=None)

        if latest_data is None:
            continue  # Skip if Yahoo Finance data unavailable
        latest_date_yf = datetime.strftime(latest_data.index[-1],"%Y-%m-%d")


        if csv_file in git_files:
            # Read existing CSV from GitHub
            git_df = read_csv_git(repo, csv_file)
            if git_df is not None:
                # Ensure index is datetime
                latest_date_git = git_df.Date.iloc[-1]
                # Check for update of differential of period
                if latest_date_yf == latest_date_git:
                    print(f"Data for {ticker} is up-to-date.")
                else:
                    print(f"Data for {ticker} is outdated. Marking for update.")
                    latest_data = get_data(ticker, mode="append", start=latest_date_git)
                    latest_data = reformat_data_yf(latest_data)
                    final_data = pd.concat([git_df, latest_data], axis=0)
                    final_data.drop_duplicates(subset="Date",inplace=True)
                    final_data.reset_index(inplace=True,drop=True)
                    updates_needed.append((ticker, final_data))
                #git_df["Date"] = pd.to_datetime(git_df["Date"])
            else:
                print(f"Failed to read {csv_file}. Downloading new data.")
                full_data = get_data(ticker, mode="update", start=None)
                full_data = reformat_data_yf(full_data)
                full_data.reset_index(inplace=True, drop=True)
                updates_needed.append((ticker, full_data))

        #If csv not in git then add the file to git
        else:
            full_data = get_data(ticker, mode="update", start=None)
            print(f"No CSV for {ticker}. Downloading new data.")
            full_data.reset_index(inplace=True)
            updates_needed.append((ticker, full_data))


    return updates_needed


def push_data_git(updates):
    """
    Push updated or new CSV files to GitHub.
    Args:
        updates: List of tuples (ticker, DataFrame) to push.
    """
    if not updates:
        print("No updates to push.")
        return

    repo = git_connect(token, repo_name)
    if not repo:
        return

    for ticker, df in updates:
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()

            # Check if file exists in repo
            csv_file = f"{ticker}.csv"
            try:
                existing_file = repo.get_contents(csv_file, ref=branch)
                # Update existing file
                repo.update_file(
                    path=csv_file,
                    message=f"Update {csv_file} with latest hourly data",
                    content=csv_content,
                    sha=existing_file.sha,
                    branch=branch
                )
                print(f"Updated {csv_file} in GitHub.")
            except:
                # Create new file
                repo.create_file(
                    path=csv_file,
                    message=f"Add {csv_file} with hourly data",
                    content=csv_content,
                    branch=branch
                )
                print(f"Created {csv_file} in GitHub.")
        except Exception as e:
            print(f"Error pushing {ticker} to GitHub: {e}")


if __name__ == "__main__":
    # Check if repo is up-to-date
    is_up_to_date, local_commit, remote_commit = check_update()
    if not is_up_to_date:
        print(f"Local repo not up-to-date. Local: {local_commit}, Remote: {remote_commit}")
        print("Please pull latest changes using: git pull origin master")
    else:
        print("Local repo is up-to-date.")

        # Process assets and push updates
        updates = process_assets()
        push_data_git(updates)