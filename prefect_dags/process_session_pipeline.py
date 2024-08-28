# from src import analysis, databasing, optimization, viz_and_reports
# from src.analysis.analysis_pipe import analysis_pipeline
from prefect import flow, task
from src.processRCS_utils.processRCS_wrapper import processRCS_wrapper
import tempfile
import polars as pl
import os
import warnings
from configs_and_globals.configs import global_config

@task
def prepare_output_dir(session):
    # Verify destination directory exists. If not, create it.
    # Using convention for directory structure: 
    # Time domain data: project_name/session_type/RCS#<side>/time_domain/session#.parquet
    # &
    # Settings: project_name/session_type/RCS#<side>/settings/session#/[time_domain, stim_settings, etc...].csv
    # Actual parquets and csvs will be written to the directories within processRCS_wrapper
    project_name = global_config["PROJECT_NAME"]
    rcs_num = session.select("Device").item()
    session_type = session.get_column("SessionType(s)").to_list()
    if len(session_type) > 1:
        warnings.warn("More than one session type found in session. Using first in the list.", UserWarning)
        session_type = session_type[0]
    else:
        session_type = session_type[0]

    output_dir = os.path.join(global_config["FILE_OUT_BASE_PATH"], session_type, rcs_num)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    session = pl.concat([session,
        pl.DataFrame({
            "parquet_path": os.path.join(output_dir, "time_domain"),
            "csv_path": os.path.join(output_dir, "settings", session.select(pl.col("Session#")).item())
        })], 
        how="horizontal"
    )

    return session


@task
def cache_session_names(session):
    # Create a temporary file to store the session DataFrame as a CSV, RCS number, and other important variables
    with tempfile.NamedTemporaryFile(mode='w+', suffix=".csv", delete=False, dir='matlab') as temp_file:
        # Write the RCS number and a newline to the file
        #temp_file.write(f"{session.select(pl.col('Device')).item()}\n")
        # Save the session DataFrame as a CSV in the temporary file
        session.write_csv(temp_file.name)
        temp_file.flush()  # Ensure all data is written to the file
        # Return the path of the temporary file for later use
        return temp_file.name

@flow
def process_session(session):

    # Prepare output directories for processed data and settings
    session = prepare_output_dir(session)

    # Get session data and run through ProcessRCS. Get combinedDataTable and settings
    sessions_tmp_file = cache_session_names(session)
    
    try:
        # Run process RCS on with tmp file available
        processRCS_wrapper()
        
        # The ingested data is assumed to be a combinedDataTable (as produced by Analysis_rcs_data.createCombinedDataTable)
        data = pl.read_parquet("file/path/to/processed/data")
    finally:
        # Delete the temporary file
        if os.path.exists(sessions_tmp_file):
            os.remove(sessions_tmp_file)
            print(f"Temporary file {sessions_tmp_file} has been deleted.")
    
    return data


# def visualize_and_database(data, analyses_output):
    
#     # Visualize the analysis results with provided aDBS policy. Use dashboard, jupyter notebook to pdf converter, and WandB for logging. Could probably just log everything to WandB, and prefect and avoid the need for custom dashboard
#     viz_and_reports.visualize(data, analyses_output) # ? rename to reporting
    
#     # Save last nights aDBS policy and reward function results.
#     databasing.save(data)


# def optimize_policy(data):
#     # Next, run optimization on the data. Get new aDBS policy for upcoming night.
#     data = optimization.optimize(data)

#     # Save new aDBS policy to optimization cache
#     pass