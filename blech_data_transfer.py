"""
- Only use user list from the server
- Cache manual selections for user convenience

- Recording log will contain:
    1) Date
    2) Time
    3) User
    4) Project name
    5) User email
    6) Recording name
    7) Recording path
"""

import easygui
import os
import argparse
import shutil
import sys
import time
from glob import glob
import pandas as pd
from tqdm import tqdm

##############################
##############################

# Load path to the blech server
script_path = os.path.dirname(os.path.realpath(__file__))
server_path_file = os.path.join(script_path, 'blech_server_path.txt')

# Get server path
if not os.path.exists(server_path_file):
    print(f"Server path file not found: {server_path_file}")
    print("I don't know how to figure out the server path.")
    print("Exiting...")
    sys.exit()
else:
    with open(server_path_file, 'r') as f:
        server_path = f.readline().strip()
    if not os.path.exists(server_path):
        print(f"Server path not found: {server_path}")
        print("Exiting...")
        sys.exit()
    else:
        print(f"Server path found: {server_path}")
        print("Continuing...")
        print("")

##############################
##############################

# Check if users list exists on the server
# If it does, copy it locally 
...

# Get list of users on the blech server
users_list_path = os.path.join(server_path, 'users_list.txt')
if not os.path.exists(users_list_path):
    print(f"Users list not found: {users_list_path}")
    print("Exiting...")
    sys.exit()
else:
    users_list = pd.read_csv(users_list_path, header=True)
    print(f"Users list found: {users_list_path}")
    print("Continuing...")
    print("")

# Ask user to select a user from the list
user_inds = list(range(len(users_list)))
user_select_str = "\n".join(f"{i}: {users_list.iloc[i]['user']}" for i in user_inds)
msg = f"Select a user from the list:\n{user_select_str}"
# Don't exit if user doesn't select a user

# Get subfolders for the selected user

# Ask user to pick a subfolder from the list, or create a new one
copy_dir = ...

# Check that selected subfolder is writable
write_bool = os.access(copy_dir, os.W_OK)
if not write_bool:
    print(f"Server path is not writable: {copy_dir}")
    print("Exiting...")
    sys.exit()
else:
    print(f"Server path is writable: {copy_dir}")
    print("Continuing...")
    print("")



##############################
##############################

# Get path to the data folder
parser = argparse.ArgumentParser(description='Transfer data from the blech server to the local machine.')
parser.add_argument('data_folder', type=str, help='Path to local data folder.',
                    default=None)
args = parser.parse_args()

if args.data_folder is None:
    data_folder = easygui.diropenbox(title='Select data folder', 
                                     default=os.path.expanduser('~/Desktop'))
else:
    data_folder = args.data_folder

print(f"Processing data folder: {data_folder}")
print("")

# Check if ".info" file exists in the data folder
info_file = glob(os.path.join(data_folder, '*.info'))
if len(info_file) == 0:
    print("""
    No ".info" file found in the data folder.
    Run blech_exp_info.py to create an ".info" file.
    Exiting...
          """)
    sys.exit()
else:
    info_file = info_file[0]
    print(f"Found .info file: {info_file}")
    print("Continuing...")
    print("")

##############################
##############################

# Begin transfer process
print("Beginning data transfer...")
print("")

# Get list of files to transfer
file_list = glob(os.path.join(data_folder, '*'))
file_list = [f for f in file_list if os.path.isfile(f)]

# Create data folder on the server
server_data_folder = os.path.join(copy_dir, os.path.basename(data_folder))
if not os.path.exists(server_data_folder):
    os.makedirs(server_data_folder)
    print(f"Created server data folder: {server_data_folder}")
    print("Continuing...")
    print("")
else:
    print(f"Server data folder already exists: {server_data_folder}")
    print("Continuing...")
    print("")

# Copy files to the server
for file in tqdm(file_list):
    if not os.path.exists(os.path.join(server_data_folder, os.path.basename(file))):
        shutil.copy2(file, server_data_folder)
    else:
        print(f"File already exists on the server: {file}")
        print("Continuing...")
        print("")
print("Data transfer complete.")
print("")

##############################
##############################

# Check if recording log exists on server 
recording_log_path = os.path.join(server_path, 'recording_log.csv')
if not os.path.exists(recording_log_path):
    print(f"Recording log not found: {recording_log_path}")
    # Ask user if this is expected, if it is, create a new recording log
    # If it's not, exit

# Copy recording log locally
shutil.copy2(recording_log_path, script_path)

# Append new entry to recording log
recording_log = pd.read_csv('recording_log.csv')
new_entry = pd.DataFrame({'date': [time.strftime('%Y-%m-%d')],
                          'time': [time.strftime('%H:%M:%S')],
                          'user': [user],
                          'project': [project],
                          'email': [email],
                          'recording': [recording],
                          'recording_path': [server_data_folder]})
recording_log = recording_log.append(new_entry, ignore_index=True)
recording_log.to_csv('recording_log.csv', index=False)

# Copy recording log back to server
shutil.copy2('recording_log.csv', server_path)
print("Recording log updated.")
print("")

##############################
##############################

print("Exiting...")
sys.exit()
