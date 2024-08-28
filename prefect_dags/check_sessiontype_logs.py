from prefect import flow, task
import json
import os
import fnmatch
import tempfile
from prefect_email import EmailServerCredentials, email_send_message
import datetime
from prefect.artifacts import create_markdown_artifact


EMAIL_ADDRESSES = ['clay.smyth@ucsf.edu', 'karena.balagula@ucsf.edu']
PATIENT_DATA_PATHS_FILE = '/home/starrlab/bin/code/rcs-database/code/database_jsons/patient_directory_names.json'

# Adapted from get_session_numbers() from https://github.com/claysmyth/rcs-database/code/manage_proj_dirs_and_csvs.py
@task
def get_sessionTypes(session_eventLog):
    # Identifies all session types logged from the SessionType window in the SCBS Report screen
    sessionTypes_tmp = []
    for entry in session_eventLog:
        # Check if the event is of type 'sessiontype'
        if entry['Event']['EventType'] == 'sessiontype':
            # Split the EventSubType by comma and remove any empty strings
            sessionTypes_single_entry = list(filter(None, entry['Event']['EventSubType'].split(", ")))
            # Add the session types from this entry to the temporary list
            sessionTypes_tmp.extend(sessionTypes_single_entry)

    # Return a list of unique session types
    return list(set(sessionTypes_tmp))



@task
def send_email_report(patient_dict):
    # Convert patient_dict to JSON
    json_report = json.dumps(patient_dict, indent=2)

    # Create a temporary file to store the JSON report
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, dir='tmp') as temp_file:
        temp_file.write(json_report)
        temp_file.flush()  # Ensure all data is written to the file
        temp_file_path = temp_file.name

    # The file will persist after the with block ends due to delete=False

        # try:
        email_server_credentials = EmailServerCredentials.load("prefect-email-credentials")
        subject = email_send_message(
            email_server_credentials=email_server_credentials,
            subject=f"Session Type Report for {datetime.date.today()}",
            msg="Attached is the report of patient sessions and identified session types... please review for any sessions that may be missing session type labels.",
            email_to=EMAIL_ADDRESSES,
            attachments=[temp_file_path],
        )

    # finally:
    # Clean up the temporary file
    os.unlink(temp_file_path)


    return


# Adapted from get_session_numbers() from https://github.com/claysmyth/rcs-database/code/cache_session_numbers.py
@flow
def check_sessiontype_logs():
    # Initialize an empty dictionary to store patient sessions
    patient_dict = {}
    
    # Open and load the patient data paths JSON file
    with open(PATIENT_DATA_PATHS_FILE) as f:
        patient_json = json.load(f)
    
    # Iterate through each device in the "Devices" section of the JSON
    for key, val in patient_json["Devices"].items():
        # Get the synced directory path for the current device
        tablet_synced_dir = val
        # Initialize a list to store session filenames for the current device
        patient_sessions = []
        
        # Iterate through all files in the synced directory
        for filename in os.listdir(tablet_synced_dir):
            # Check if the filename matches the pattern 'Session*'
            # Check if the filename starts with 'Session'
            if fnmatch.fnmatch(filename, 'Session*'):
                # Construct the full path to the session directory
                session_dir = os.path.join(tablet_synced_dir, filename)
                # Look for 'Device' subdirectory within the Session directory
                device_dirs = [d for d in os.listdir(session_dir) if d.startswith('Device')]
                if device_dirs:
                    # If 'Device' subdirectory exists, construct path to it
                    device_dir = os.path.join(session_dir, device_dirs[0])
                    # Construct path to EventLog.json file
                    eventlog_path = os.path.join(device_dir, 'EventLog.json')
                    if os.path.exists(eventlog_path):
                        # If EventLog.json exists, read its contents
                        with open(eventlog_path, 'r') as f:
                            session_eventLog = json.load(f)
                        # Extract session types from the event log
                        session_types = get_sessionTypes(session_eventLog)
                        # Add session filename and types to patient_sessions list
                        patient_sessions.append((filename, session_types))
                    else:
                        # If EventLog.json doesn't exist, add session with empty types list
                        patient_sessions.append((filename, []))
                else:
                    # If 'Device' subdirectory doesn't exist, add session with empty types list
                    patient_sessions.append((filename, []))
        
        # If sessions were found for this device, add them to the patient_dict
        if patient_sessions:
            patient_dict[key] = patient_sessions
    
    # Send email with patient_dict as attachment
    send_email_report(patient_dict)

    # Log the patient_dict as a markdown artifact in Prefect
    create_markdown_artifact(
        key="session-type-report",
        markdown=f"```json\n{json.dumps(patient_dict, indent=2, sort_keys=True)}\n```",
        description=f"Session Type Report for {datetime.date.today()}",
    )

    # Return the dictionary of devices and their associated session filenames
    return patient_dict


if __name__ == "__main__":
    _ = check_sessiontype_logs()