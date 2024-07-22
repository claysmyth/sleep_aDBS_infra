import subprocess
from configs_and_globals.configs import global_config
from prefect import task

@task
def processRCS_wrapper(command):
    try:
        # Execute the terminal command
        command = global_config.MATLAB_PROCESS_SESSION_COMMAND
        subprocess.run(command, shell=True, check=True)
        print("Command executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        
# Moves session data from synced to unsynced
