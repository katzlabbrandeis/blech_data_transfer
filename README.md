![image](https://github.com/user-attachments/assets/cb03c5c9-da50-40fa-9fbe-99e29376615c)
Courtesy of ChatGPT

# Introduction
Code to:
1. Transfer ephys data from the local machine to the server
    - Recording will be copied to directory of specified user, under a specific project name
2. Check that info file is present
3. Update recording log

NOTE: This system will only work well if this method of copying cannot be side-stepped
    by the user without including metadata.
    Alternatively, there could be functionality to keep emailing the user if a dataset is uploaded
    (after a certain date, so previous datasets are not affected) without metadata.
    (see https://www.sitepoint.com/quick-tip-sending-email-via-gmail-with-python/)

- Mounting of server is required, but not included in this code
    - Code is given path to the server
- If info file is not present, transfer will not be allowed
- Copy of recording log is made locally and updated with new recording
    - This allows multiple copies of the recording log to be made in case of accidental deletion
- Recording log will contain:
    1) Date
    2) Time
    3) User
    4) Project name
    5) User email
    6) Recording name
    7) Recording path
- Directory to be copied can be specified both via the command line and via popup

# How to use

## blech_data_transfer.py
```
usage: python blech_data_transfer.py [-h] data_folder

Transfer data from the blech server to the local machine.

positional arguments:
  data_folder  Path to local data folder.

options:
  -h, --help   show this help message and exit
```

## mount_katz_drive.sh
First install `cifs-utils` ::: `sudo apt-get install cifs-utils`
```
usage: ./mount_katz_drive.sh
```
- Will first ask for machine sudo password, then for brandeis password.
- Will mount the katz drive to the local machine at /media/files_brandeis_drive by default.
- Edit the script to change the mount location.

# Moonshot
- Autoprocess uploaded data and extract recording quality features such as:
    - Number of units
    - Unit amplitude
    - Unit signal-to-noise ratio
    - Mean firing rate per unit
    - Responsive fraction
    - Discriminative fraction
    - Palatable fraction
    - Dynamic fraction
    - Drift descriptors
    - Unit similarity 

# How to use

## blech_data_transfer.py
```
usage: python blech_data_transfer.py [-h] data_folder

Transfer data from the blech server to the local machine.

positional arguments:
  data_folder  Path to local data folder.

options:
  -h, --help   show this help message and exit
```

## mount_katz_drive.sh
```
usage: ./mount_katz_drive.sh
```
Will first ask for machine sudo password, then for brandeis password.
Will mount the katz drive to the local machine at /media/files_brandeis_drive by default.
Edit the script to change the mount location.
