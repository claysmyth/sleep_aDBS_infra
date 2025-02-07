# from src import analysis, databasing, optimization, viz_and_reports
# from src.analysis.analysis_pipe import analysis_pipeline
from prefect import flow, task
import sys
import os
from src.processRCS_utils.processRCS_wrapper import processRCS_wrapper
import tempfile
import polars as pl
import os
import glob
import warnings

# from prefect import get_run_logger
# from configs_and_globals.configs import global_config

# Use absolute path to avoid issues with cron job changing directory
TEMP_DIR = os.path.abspath("/home/claysmyth/code/sleep_aDBS_infra/matlab")


@task
def prepare_output_dirs_deprecated(session, global_config):
    # Verify destination directory exists. If not, create it.
    # Using convention for directory structure:
    # Time domain data: project_name/session_type/RCS#<side>/time_domain/session#.parquet
    # &
    # Settings: project_name/session_type/RCS#<side>/settings/session#/[time_domain, stim_settings, etc...].csv
    # Actual parquets and csvs will be written to the directories within processRCS_wrapper
    rcs_num = session.select("Device").item()
    session_type = session.get_column("SessionType(s)").to_list()
    if len(session_type) > 1:
        warnings.warn(
            "More than one session type found in session. Using first in the list.",
            UserWarning,
        )
        session_type = session_type[0]
    else:
        session_type = session_type[0]

    output_dir = os.path.join(
        global_config["FILE_OUT_BASE_PATH"], session_type, rcs_num
    )
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    session = pl.concat(
        [
            session,
            pl.DataFrame(
                {
                    "parquet_path": os.path.join(output_dir, "time_domain_data"),
                    "settings_path": os.path.join(
                        output_dir,
                        "session_settings",
                        session.select(pl.col("Session#")).item(),
                    ),
                }
            ),
        ],
        how="horizontal",
    )

    return session


@task
def prepare_output_dirs(sessions_info, global_config):
    # Verify destination directory exists. If not, create it.
    # Using convention for directory structure:
    # Time domain data: project_name/session_type/RCS#<side>/time_domain/session#.parquet
    # &
    # Settings: project_name/session_type/RCS#<side>/settings/session#/[time_domain, stim_settings, etc...].csv
    # Actual parquets and csvs will be written to the directories within processRCS_wrapper

    sessions_info = sessions_info.with_columns(
        pl.concat_str(
            [
                pl.lit(global_config["FILE_OUT_BASE_PATH"]),
                pl.col("SessionType(s)"),
                pl.col("Device"),
            ],
            separator="/",
        ).alias("output_dir")
    ).with_columns(
        pl.concat_str(
            [
                pl.col("output_dir"),
                pl.lit("time_domain_data"),
                pl.col("Session#") + ".parquet",
            ],
            separator="/",
        ).alias("parquet_path"),
        pl.concat_str(
            [pl.col("output_dir"), pl.lit("session_settings"), pl.col("Session#")],
            separator="/",
        ).alias("settings_path"),
    )

    # Verify destination directory exists. If not, create it.
    for output_dir in sessions_info.get_column("output_dir").to_list():
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    return sessions_info


@task
def remove_matlab_temp_files(tmp_dir="matlab"):
    # Remove temporary CSV files in the 'matlab' directory, to start fresh each time
    temp_files = glob.glob(os.path.join(tmp_dir, "tmp*.csv"))
    for file in temp_files:
        os.remove(file)


@task
def cache_session_info(session, tmp_dir=TEMP_DIR):
    # Create a temporary file to store the session DataFrame as a CSV, RCS number, and other important variables
    try:
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".csv", delete=False, dir=tmp_dir
        ) as temp_file:
            # Write the RCS number and a newline to the file
            # temp_file.write(f"{session.select(pl.col('Device')).item()}\n")
            # Save the session DataFrame as a CSV in the temporary file
            session.write_csv(temp_file.name)
            temp_file.flush()  # Ensure all data is written to the file
            # Return the path of the temporary file for later use
            return temp_file.name
    except Exception as e:
        print(f"An error occurred while caching session info with tmp file: {str(e)}")
        return None


@flow(log_prints=True)
def process_session(session, global_config):

    # Remove temporary CSV files in the 'matlab' directory
    remove_matlab_temp_files(TEMP_DIR)

    # Get session data and run through ProcessRCS. Get combinedDataTable and settings
    sessions_tmp_file = cache_session_info(session, TEMP_DIR)

    if sessions_tmp_file is None:
        return None

    try:
        # Run process RCS on with tmp file available. processRCS_wrapper will read the tmp file to get the session info.
        # Assumes that the tmp file will be in the same directory as the matlab executable
        success = processRCS_wrapper(global_config)

        if not success:
            raise Exception("processRCS_wrapper failed to execute successfully")

        # The ingested data is assumed to be a combinedDataTable (as produced by Analysis_rcs_data.createCombinedDataTable)
        data = pl.read_parquet(session.get_column("parquet_path").item()).sort(
            "localTime"
        )

    except Exception as e:
        print(f"An error occurred while processing the session: {str(e)}")
        data = None

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
