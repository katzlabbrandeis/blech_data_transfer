Code to:
    1) Transfer ephys data from the local machine to the server
        - Recording will be copied to directory of specified user, under a specific project name
    2) Check that info file is present
    3) Update recording log

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
