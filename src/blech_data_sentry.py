"""
Scan the file-system for any datasets and check if they have accompanying metadata.

This script searches through the server directory structure to find all datasets
(identified by info.rhd files) and checks if they have the required metadata files
(*.info). The results are saved to a CSV file for tracking purposes.

If metadata is missing, this information can be used to contact users to request
the missing metadata.
"""

import os
from glob import glob
from tqdm import tqdm
import pandas as pd
from datetime import datetime
from time import time
import argparse
from src.utils.utils import base_dir_path as dir_path 


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
            description='Scan the file-system for any datasets and see if they have accompanied metadata')
    parser.add_argument('--ignore_blacklist', action='store_true', help='Ignore the blacklist file')
    return parser.parse_args()


def get_server_path(dir_path):
    """Get and validate the server path from the configuration file"""
    server_path_file = os.path.join(dir_path, 'local_only_files', 'blech_server_path.txt')
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
    return server_path


def setup_server_home_dir(server_path):
    """Set up the server home directory"""
    server_home_dir = os.path.join(server_path, 'data_management')
    if not os.path.exists(server_home_dir):
        print(f'Creating server home directory: {server_home_dir}')
        os.mkdir(server_home_dir)
    else:
        print(f'Server home directory already exists: {server_home_dir}')
    return server_home_dir


def handle_blacklist(server_home_dir, dir_path, ignore_blacklist):
    """Handle the blacklist file and return the blacklist and blacklist file path"""
    if not ignore_blacklist:
        # Look for blacklist file in both server_home_dir and dir_path
        blacklist_file_paths = [
                os.path.join(server_home_dir, 'sentry_blacklist.txt'),
                os.path.join(dir_path, 'sentry_blacklist.txt')
                ]
        for f in blacklist_file_paths:
            if os.path.exists(f):
                blacklist_file = f
                break
        else:
            print('Blacklist file not found')
            print('Continuing without blacklist')
            return [], None, 'None'

        print(f'Blacklist file: {blacklist_file}')
        print()

        # Read blacklist file
        with open(blacklist_file, 'r') as f:
            blacklist = f.read().splitlines()
        blacklist_str = '\n'.join(blacklist)
        print('Blacklist:\n'+'========='+'\n'+blacklist_str) 
        print()
        return blacklist, blacklist_file, blacklist_str
    else:
        blacklist = []
        blacklist_str = 'None'
        print('Ignoring blacklist')
        print()
        return blacklist, None, blacklist_str


def get_directories_to_scan(server_path, blacklist):
    """Get list of directories to scan"""
    top_level_dirs = [d for d in os.listdir(server_path) if os.path.isdir(os.path.join(server_path, d))]
    # Remove blacklist directories
    if blacklist:
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
    
    return sub_dirs, top_level_dirs_str


def scan_for_info_files(server_path, sub_dirs):
    """Scan for info.rhd files in subdirectories recursively"""
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
    
    return info_file_paths


def create_dataset_frame(info_file_paths, server_path):
    """Create a dataframe from the info file paths"""
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
    return dataset_frame, data_dirs


def check_metadata(data_dirs):
    """Check if metadata exists for each dataset"""
    print(f'Scanning for metadata files')
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
    
    return metadata_files, metadata_present


def write_results(dataset_frame, server_home_dir, start_time, blacklist_str, top_level_dirs_str):
    """Write results to files"""
    now = datetime.now()
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

    end_time = time()
    time_taken = end_time - start_time
    print(f'Writing dataset_frame to csv file')
    out_path = os.path.join(server_home_dir, f'dataset_frame.csv')
    print(f'Output path: {out_path}')
    dataset_frame.to_csv(out_path)
    print(f'Writing to log file : {server_home_dir}/last_scan.txt')
    with open(os.path.join(server_home_dir, 'last_scan.txt'), 'w') as f:
        f.write(date_time)
        f.write('\n\n')
        f.write(f'Time taken: {(time_taken)/60:.2f} minutes')
        f.write('\n\n')
        f.write('Blacklist:\n'+blacklist_str)
        f.write('\n\n')
        f.write('Top level directories processed:\n'+top_level_dirs_str)


def main():
    """Main function to run the script"""
    args = parse_arguments()
    start_time = time()
    
    # Get and validate server path
    server_path = get_server_path(dir_path)
    
    # Set up server home directory
    server_home_dir = setup_server_home_dir(server_path)
    
    # Handle blacklist
    blacklist, blacklist_file, blacklist_str = handle_blacklist(server_home_dir, dir_path, args.ignore_blacklist)
    
    # Get directories to scan
    sub_dirs, top_level_dirs_str = get_directories_to_scan(server_path, blacklist)
    
    # Scan for info.rhd files
    info_file_paths = scan_for_info_files(server_path, sub_dirs)
    
    # Create dataset frame
    dataset_frame, data_dirs = create_dataset_frame(info_file_paths, server_path)
    
    # Check metadata
    metadata_files, metadata_present = check_metadata(data_dirs)
    dataset_frame['metadata_file'] = metadata_files
    dataset_frame['metadata_present'] = metadata_present
    
    # Write results
    write_results(dataset_frame, server_home_dir, start_time, blacklist_str, top_level_dirs_str)


if __name__ == "__main__":
    main()
