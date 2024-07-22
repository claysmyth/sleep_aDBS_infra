from src import analysis, databasing, optimization, visualization
from prefect import flow, task
from src.processRCS_utils import processRCS_wrapper
import tempfile
import polars as pl

@task
def cache_session_names(session, rcs_num):
    # Create a temporary file to store the session DataFrame as a CSV, RCS number, and other important variables
    with tempfile.NamedTemporaryFile(mode='w+', suffix=".csv", delete=True, dir='matlab') as temp_file:
        # Write the RCS number and a newline to the file
        temp_file.write(f"{rcs_num}\n")
        # Save the session DataFrame as a CSV in the temporary file
        session.write_csv(temp_file, index=False)
        temp_file.flush()  # Ensure all data is written to the file
        # Return the path of the temporary file for later use
        return temp_file.name

@flow
def process_sessions(session):
    
    # First, get session data and run through ProcessRCS. Get combinedDataTable and settings
    sessions_file = cache_session_names(session)
    
    # Run process RCS on with tmp file available
    processRCS_wrapper()
    data = pl.read_parquet("file/path/to/processed/data")
    
    # Next, run analysis on the data. Get delta power, etc..
    analyses = analysis.analysis_pipe(data)
    
    # Visualize the analysis results with provided aDBS policy. Use dashboard, jupyter notebook to pdf converter, and WandB for logging. Could probably just log everything to WandB, and prefect and avoid the need for custom dashboard
    visualization.visualize(data, analyses)
    
    # Save last nights aDBS policy and reward function results.
    databasing.save(data)
    
    # Next, run optimization on the data. Get new aDBS policy for upcoming night.
    data = optimization.optimize(data)
    
    # Save new aDBS policy to optimization cache
    pass