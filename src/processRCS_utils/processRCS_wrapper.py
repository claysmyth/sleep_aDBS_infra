import subprocess
#from configs_and_globals.configs import global_config
from prefect import task

@task
def processRCS_wrapper(global_config):
    try:
        # Execute the shell script
        script_path = global_config["MATLAB_SHELL_COMMAND"]
        result = subprocess.run(["bash", script_path], check=True, capture_output=True, text=True)
        print("MATLAB process script executed successfully.")
        # Log the output
        print("Script Output:", result.stdout)
        print("Script Errors:", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error executing MATLAB process script: {e}")
        print("Script Output:", e.stdout)
        print("Script Errors:", e.stderr)

