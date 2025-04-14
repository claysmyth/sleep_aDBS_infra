from prefect import flow, task
import json
import os
import fnmatch
import tempfile
from prefect_email import EmailServerCredentials, email_send_message
import datetime
from prefect.artifacts import create_markdown_artifact
import yagmail
from pathlib import Path
from dotenv import load_dotenv
import pprint

# This script checks the session EventLog.json for a sessiontype. It is typically run via crontab at 10:00am to check for any sessions that may be missing session type labels, and emailed to the desired users.

# Email addresses to send the report to
EMAIL_ADDRESSES = ["clay.smyth@ucsf.edu", "karena.balagula@ucsf.edu"]
# Path to the patient data paths JSON file, to search within synced directories for new sessions
PATIENT_DATA_PATHS_FILE = "/home/starrlab/bin/code/rcs-database/code/database_jsons/patient_directory_names.json"
# Where to store temporary files
TMP_DIR = "/home/claysmyth/code/sleep_aDBS_infra/tmp"

# Switched to loading from environment variables instead of JSON file.
# If you need to revert to JSON file, add filepath and edit load_gmail_credentials call in send_email_report().
GMAIL_CREDENTIALS_PATH = Path("##")


def load_gmail_credentials():
    try:
        with GMAIL_CREDENTIALS_PATH.open("r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise RuntimeError(f"Failed to load Gmail credentials: {e}")


def load_gmail_credentials_env():
    load_dotenv()
    username = os.getenv("GMAIL_USERNAME")
    password = os.getenv("GMAIL_PASSWORD")
    if not username or not password:
        raise ValueError("Gmail credentials not found in environment variables")
    return {"username": username, "password": password}


# Adapted from get_session_numbers() from https://github.com/claysmyth/rcs-database/code/manage_proj_dirs_and_csvs.py
@task
def get_sessionTypes(session_eventLog):
    # Identifies all session types logged from the SessionType window in the SCBS Report screen
    sessionTypes_tmp = []
    for entry in session_eventLog:
        # Check if the event is of type 'sessiontype'
        if entry["Event"]["EventType"] == "sessiontype":
            # Split the EventSubType by comma and remove any empty strings
            sessionTypes_single_entry = list(
                filter(None, entry["Event"]["EventSubType"].split(", "))
            )
            # Add the session types from this entry to the temporary list
            sessionTypes_tmp.extend(sessionTypes_single_entry)

    # Return a list of unique session types
    return list(set(sessionTypes_tmp))


@task(retries=5, retry_delay_seconds=60)
def send_email_report(patient_dict):
    # Convert patient_dict to JSON
    json_report = json.dumps(patient_dict, indent=2)

    # Create a temporary file to store the JSON report
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, dir=TMP_DIR
    ) as temp_file:
        temp_file.write(json_report)
        temp_file.flush()  # Ensure all data is written to the file
        temp_file_path = temp_file.name

        # The file will persist after the 'with' block ends due to delete=False

        # Prefect email approach.. stopped working after upgrading to Prefect 3.0 but keeping for now in case it starts working again.
        # Need to run async code in a task to get this to work.
        # This would be the preferred approach if it worked. It's a bit safer than yagmail.
        # Apparently breaks because I implement await and async incorrectly??
        # email_server_credentials = EmailServerCredentials.load("prefect-email-credentials")
        # subject = email_send_message(
        #     email_server_credentials=email_server_credentials,
        #     subject=f"Session Type Report for {datetime.date.today()}",
        #     msg="Attached is the report of patient sessions and identified session types... please review for any sessions that may be missing session type labels.",
        #     email_to=EMAIL_ADDRESSES,
        #     attachments=[temp_file_path],
        # )

        # Read Gmail credentials from JSON file
        try:
            gmail_creds = load_gmail_credentials_env()

            # Initialize yagmail SMTP client with credentials
            yag = yagmail.SMTP(gmail_creds["username"], gmail_creds["password"])

            # Send email with JSON contents in body
            yag.send(
                to=EMAIL_ADDRESSES,
                subject=f"Session Type Report for {datetime.date.today()}",
                contents=f"""Attached is the report of patient sessions and identified session types... please review for any sessions that may be missing session type labels.
                    File sizes are in MB.
                    
                    Report contents:
                    {json.dumps(patient_dict, indent=4)}
                    """,
                attachments=[temp_file_path],
            )
        except Exception as e:
            print(f"Failed to send email: {e}")
            raise

    # finally:
    # Clean up the temporary file
    os.unlink(temp_file_path)

    return


# Adapted from get_session_numbers() from https://github.com/claysmyth/rcs-database/code/cache_session_numbers.py
@flow(log_prints=True)
def check_sessiontype_logs():
    # Initialize an empty dictionary to store patient sessions
    patient_dict = {}

    # Open and load the patient data paths JSON file
    with open(PATIENT_DATA_PATHS_FILE) as f:
        patient_json = json.load(f)

    # Iterate through each device in the "Devices" section of the JSON
    for key, val in patient_json["Devices"].items():
        print(f"Checking device {key}")
        # Get the synced directory path for the current device
        tablet_synced_dir = val
        # Initialize a list to store session filenames for the current device
        patient_sessions = []

        # Iterate through all files in the synced directory
        for filename in os.listdir(tablet_synced_dir):
            # Check if the filename matches the pattern 'Session*'
            # Check if the filename starts with 'Session'
            if fnmatch.fnmatch(filename, "Session*"):
                print(f"In device {key}, found session {filename}")
                # Construct the full path to the session directory
                session_dir = os.path.join(tablet_synced_dir, filename)

                # Add session filename and types to patient_sessions list
                unix_timestamp = int(filename.replace("Session", ""))
                local_datetime = datetime.datetime.fromtimestamp(
                    unix_timestamp // 1000
                ).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )  # Convert from milliseconds to seconds with //1000

                # Look for 'Device' subdirectory within the Session directory
                device_dirs = [
                    d for d in os.listdir(session_dir) if d.startswith("Device")
                ]
                if device_dirs:
                    # If 'Device' subdirectory exists, construct path to it
                    device_dir = os.path.join(session_dir, device_dirs[0])
                    # Construct path to EventLog.json file
                    eventlog_path = os.path.join(device_dir, "EventLog.json")
                    if os.path.exists(eventlog_path):

                        # If EventLog.json exists and is well-formed, read its contents
                        with open(eventlog_path, "r") as f:
                            try:
                                session_eventLog = json.load(f)
                            except json.JSONDecodeError as e:
                                err_string = f"Error decoding JSON from {eventlog_path}: {e} for session {filename} for device {key}"
                                print(err_string)
                                patient_sessions.append(
                                    {
                                        filename: {
                                            "start_time": local_datetime,
                                            "error": err_string,
                                            "sessiontypes": "Cannot Determine",
                                            "file_sizes": {},
                                        }
                                    }
                                )
                                continue

                        # Extract session types from the event log
                        session_types = get_sessionTypes(session_eventLog)

                        # Check the size of EventLog.json and RawDataTD.json
                        eventlog_size = os.path.getsize(eventlog_path) / (
                            1024 * 1024
                        )  # Convert to MB
                        rawtddata_path = os.path.join(device_dir, "RawDataTD.json")
                        rawtddata_size = (
                            os.path.getsize(rawtddata_path) / (1024 * 1024)
                            if os.path.exists(rawtddata_path)
                            else None
                        )  # Convert to MB if file exists

                        # Add file sizes to the session information
                        file_sizes = {
                            "EventLog.json": eventlog_size,
                            "RawDataTD.json": rawtddata_size,
                        }

                        patient_sessions.append(
                            {
                                filename: {
                                    "start_time": local_datetime,
                                    "sessiontypes": session_types,
                                    "file_sizes": file_sizes,
                                }
                            }
                        )
                    else:
                        print(
                            f"EventLog.json not found for session {filename} in device {key}"
                        )
                        # If EventLog.json doesn't exist, add session with empty types list
                        patient_sessions.append(
                            {
                                filename: {
                                    "start_time": local_datetime,
                                    "sessiontypes": [],
                                    "file_sizes": file_sizes,
                                }
                            }
                        )
                else:
                    print(
                        f"'Device' subdirectory not found for session {filename} in device {key}"
                    )
                    # If 'Device' subdirectory doesn't exist, add session with empty types list
                    patient_sessions.append(
                        {
                            filename: {
                                "start_time": local_datetime,
                                "sessiontypes": [],
                                "file_sizes": file_sizes,
                            }
                        }
                    )

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


def load_gmail_credentials():
    try:
        with GMAIL_CREDENTIALS_PATH.open("r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise RuntimeError(f"Failed to load Gmail credentials: {e}")


if __name__ == "__main__":
    _ = check_sessiontype_logs()
