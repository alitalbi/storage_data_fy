# upload_data.py
from data import check_update, process_assets, push_data_git

def main():
    """
    Main script to check repo status, process assets, and update GitHub.
    """
    print("Starting data update process...")

    # Step 1: Check if local repo is up-to-date
    is_up_to_date, local_commit, remote_commit = check_update()
    if not is_up_to_date:
        print(f"Local repo not up-to-date. Local: {local_commit}, Remote: {remote_commit}")
        print("Please run 'git pull origin master' and try again.")
        return

    # Step 2: Process assets and identify updates
    updates = process_assets()
    print(f"Found {len(updates)} assets needing updates.")

    # Step 3: Push updates to GitHub
    push_data_git(updates)
    print("Data update process completed.")

if __name__ == "__main__":
    main()