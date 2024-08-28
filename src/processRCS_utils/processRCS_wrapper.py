import subprocess
from configs_and_globals.configs import global_config
from prefect import task

@task
def processRCS_wrapper():
    try:
        # Execute the terminal command
        command = global_config["MATLAB_PROCESS_SESSION_COMMAND"]
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print("MATLAB command executed successfully.")
        # Optionally, you can log the output
        print("MATLAB Output:", result.stdout)
        print("MATLAB Errors:", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error executing MATLAB command: {e}")
        print("MATLAB Output:", e.output)
        print("MATLAB Errors:", e.stderr)

