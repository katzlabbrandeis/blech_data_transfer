"""
Scan the file-system for any datasets and see if they have accompanied metadata
If not, email user to request metadata
"""

import os
from glob import glob
from tqdm import tqdm
import pandas as pd
from datetime import datetime
from time import time
import argparse

parser = argparse.ArgumentParser(
        description='Scan the file-system for any datasets and see if they have accompanied metadata')
parser.add_argument('--ignore_blacklist', action='store_true', help='Ignore the blacklist file')
args = parser.parse_args()

start_time = time()
script_path = os.path.realpath(__file__)
dir_path = os.path.dirname(script_path)

# dir_path = '/media/bigdata/projects/blech_data_transfer'
# get blech_server_path.txt
server_path_file = os.path.join(dir_path, 'blech_server_path.txt')
with open(server_path_file, 'r') as f:
    server_path = f.read().strip()

# Check that server_path exists and is accessible
if not os.path.exists(server_path):
    print('Server path does not exist')
    exit()
if not os.access(server_path, os.R_OK):
    print('Server path is not accessible')
    exit()

print(f'Server path file: {server_path_file}')
print(f'Server path: {server_path}')

server_home_dir = os.path.join(server_path, 'data_management')
if not os.path.exists(server_home_dir):
    os.mkdir(server_home_dir)

print(f'Server home directory: {server_home_dir}')

if not args.ignore_blacklist:
    # Look for blacklist file in both server_home_dir and dir_path
    blacklist_file = [
            os.path.join(server_home_dir, 'sentry_blacklist.txt'),
            os.path.join(dir_path, 'sentry_blacklist.txt')
            ]
    for f in blacklist_file:
        if os.path.exists(f):
            blacklist_file = f
            break
    else:
        print('Blacklist file not found')
        print('Continuing without blacklist')

    print(f'Blacklist file: {blacklist_file}')
    print()

    # Read blacklist file
    if blacklist_file:
        with open(blacklist_file, 'r') as f:
            blacklist = f.read().splitlines()
        blacklist_str = '\n'.join(blacklist)
        print('Blacklist:\n'+blacklist_str) 
        print()
else:
    blacklist = []
    blacklist_str = 'None'
    print('Ignoring blacklist')
    print()


# Look for 'info.rhd' files
# Get list of all directories upto 2 levels deep first so we can have a progress bar
top_level_dirs = [d for d in os.listdir(server_path) if os.path.isdir(os.path.join(server_path, d))]
# Remove blacklist directories
if blacklist_file:
    top_level_dirs = [d for d in top_level_dirs if d not in blacklist]

top_level_dirs_str = '\n'.join(top_level_dirs)
print('Top level directories to scan:\n'+ top_level_dirs_str)
print()

sub_dirs = []
for d in top_level_dirs:
    sub_dirs.extend(
            [os.path.join(d, sd) for sd in os.listdir(os.path.join(server_path, d)) \
                    if os.path.isdir(os.path.join(server_path, d, sd))
             ]
            )

# Scan for info.rhd files in subdirectories recursively
info_file_paths = []
scanned_sub_dirs = []

print('Scanning for info.rhd files')
pbar = tqdm(sub_dirs)
for d in pbar: 
    if d in scanned_sub_dirs:
        continue
    pbar.set_description(f'Scanning {d}')
    this_dir = os.path.join(server_path, d)
    info_files = glob(os.path.join(this_dir, '**', 'info.rhd'), recursive=True)
    if info_files:
        info_file_paths.extend(info_files)
    scanned_sub_dirs.append(d)

# Get directories with info.rhd files
root_dir = [x.split(server_path)[1].split('/')[1] for x in info_file_paths] 
data_dirs = [os.path.dirname(f) for f in info_file_paths]

# Create dataframe 
dataset_frame = pd.DataFrame(
        dict(
            root_dir=root_dir,
            data_dir=[os.path.relpath(d, server_path) for d in data_dirs],
            )
        )

print(f'Found {len(dataset_frame)} datasets')
print(f'Scanning for metadata files')
# Check if metadata exists for each dataset
metadata_pattern = '*.info'
metadata_files = []
metadata_present = []
for d in tqdm(data_dirs):
    check_path = os.path.join(d, metadata_pattern)
    glob_out = glob(check_path)
    if glob_out:
        rel_paths = [os.path.relpath(p, d) for p in glob_out]
        metadata_files.append(rel_paths)
        metadata_present.append(True)
    else:
        metadata_files.append(None)
        metadata_present.append(False)

dataset_frame['metadata_file'] = metadata_files
dataset_frame['metadata_present'] = metadata_present

# Write out dataset_frame to a csv file along with the date-time
now = datetime.now()
date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

end_time = time()
time_taken = end_time - start_time
print(f'Writing dataset_frame to csv file')
out_path = os.path.join(server_home_dir, f'dataset_frame.csv')
dataset_frame.to_csv(out_path)
with open(os.path.join(server_home_dir, 'last_scan.txt'), 'w') as f:
    f.write(date_time)
    f.write('\n\n')
    f.write(f'Time taken: {(time_taken)/60:.2f} minutes')
    f.write('\n\n')
    f.write('Blacklist:\n'+blacklist_str)
    f.write('\n\n')
    f.write('Top level directories processed:\n'+top_level_dirs_str)
