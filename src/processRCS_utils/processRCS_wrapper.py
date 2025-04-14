import subprocess

# from configs_and_globals.configs import global_config
from prefect import task


@task
def processRCS_wrapper(global_config, timeout=900):  # Default timeout of 900 seconds (15 minutes)
    try:
        script_path = global_config["MATLAB_SHELL_COMMAND"]
        result = subprocess.run(
            ["bash", script_path],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,  # Timeout in seconds
        )
        print("MATLAB process script executed successfully.")
        print("Script Output:", result.stdout)
        print("Script Errors:", result.stderr)
        return True
    except subprocess.TimeoutExpired as e:
        print(f"MATLAB process script timed out after {timeout} seconds.")
        print("Partial Output:", e.stdout)
        print("Partial Errors:", e.stderr)
        return False
    except subprocess.CalledProcessError as e:
        print(f"Error executing MATLAB process script: {e}")
        print("Script Output:", e.stdout)
        print("Script Errors:", e.stderr)
        return False
