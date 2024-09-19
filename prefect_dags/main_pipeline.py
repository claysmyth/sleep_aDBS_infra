from prefect import flow, task, tags
import polars as pl
from process_session_pipeline import process_session, prepare_output_dirs
import os
from src.analysis.analysis_pipe import AnalysisPipe
from viz_and_reporting_pipeline import VisualizationAndReportingPipeline
# from configs_and_globals.configs import global_config
from omegaconf import DictConfig, OmegaConf
import hydra

SESSION_TYPES_TO_IGNORE = ['Autorun']


@task
def get_new_sessions(project_df, reported_sessions_df):
    if reported_sessions_df.is_empty():
        return project_df
    else:
        return project_df.join(reported_sessions_df, on="Session#", how="anti")
    

# Aggregate sessions into groups based upon criteria function defined in configs_and_globals/analysis_config/default.yaml
@task
def agg_sessions_subpipe(analysis_pipe, sessions_subset_info, sessions_data):
    sessions_subset_info = analysis_pipe.run_aggregation_criteria(sessions_subset_info, sessions_data)
    session_info_grouped, grouped_data = analysis_pipe.update_data_with_aggregation_criteria(sessions_subset_info, sessions_data)
    return session_info_grouped, grouped_data

    
@flow
def main_pipeline(cfg: DictConfig):

    # Convert DictConfig to a dictionary
    config = OmegaConf.to_container(cfg, resolve=True)
    global_config = config["global_config"]

    # Read in table that contains sessions logged to desired project via (insert github link here)
    project_df = pl.read_csv(global_config["PROJECT_CSV_PATH"])
    
    # Read in table that contains sessions which have already been reported     
    # Check if the file exists, if not create an empty CSV
    if os.path.exists(global_config["REPORTED_SESSIONS_CSV_PATH"]):
        reported_sessions_df = pl.read_csv(global_config["REPORTED_SESSIONS_CSV_PATH"])
    else:
        reported_sessions_df = pl.DataFrame()
    
    # Get sessions which have not been reported yet
    sessions_info = get_new_sessions(project_df, reported_sessions_df)
    
    # Could create different AnalysisPipes, VisualizationPipes, and ReportingPipes for different projects or session_types.
    # This would require parameterizing the analysis config and passing it into the AnalysisPipe class (same for visualization and reporting).
        # Could manage AnalysisPipes configs with hydra?
    # Currently, only one AnalysisPipe, VisualizationPipe, and ReportingPipe is used for all projects and session types.
    analysis_pipe = AnalysisPipe(config["analysis_config"])
    visualization_pipe = VisualizationAndReportingPipeline(config["viz_and_reporting_config"])
    
    # It's useful to have a device column as a unit for analysis and visualization
    sessions_info = sessions_info.with_columns((pl.col('RCS#') + pl.col('Side').str.slice(0, 1)).alias('Device'))

    # Ignore specific session types for now
    sessions_info = sessions_info.filter(~pl.col('SessionType(s)').is_in(SESSION_TYPES_TO_IGNORE))

    # Prepare output directories for processed data and settings
    sessions_info = prepare_output_dirs(sessions_info, global_config)

    for sessions_subset_info in sessions_info.partition_by(['Device', 'SessionType(s)']):

        with tags(sessions_subset_info.get_column('Device')[0], sessions_subset_info.get_column('SessionType(s)')[0]):
            sessions_subset_info = sessions_subset_info.with_columns(pl.col('TimeStarted').str.to_datetime(format="%m-%d-%Y %H:%M:%S")).sort('TimeStarted')
            sessions_data = []
            for i in range(sessions_subset_info.height):
                session_data = process_session(sessions_subset_info[i], global_config)
                sessions_data.append(session_data)
            
            # Run aggregation criteria on sessions... otherwise analyze, viz, and bayesian optimize each session individually.
            if sessions_subset_info.height > 1:
                # Check if sessions are from the same 'unit' or 'group' of analysis (e.g. multiple recordings in a single night of sleep for sleep project)
                # sessions_subset_info is a polars DataFrame with a 'Group' column indicating the group of analysis for each session
                session_info_grouped, grouped_data = agg_sessions_subpipe(analysis_pipe, sessions_subset_info, sessions_data)

            else:
                grouped_data = sessions_data
                session_info_grouped = [sessions_subset_info.with_columns(pl.lit(0).alias('Group'))]

            # sessions_info_grouped is a list of polars DataFrames, where a row in each dataframe is a session
            # grouped_data is a list of polars DataFrames, where each DataFrame contains the session data for a group of sessions (concatenated vertically)

            # Next, run analysis on the data. Get delta power, etc.. Each analysis result is a key: value pair of function name and polars DataFrame
            for i, data in enumerate(grouped_data):
                # Next, run analysis on the data. Get delta power, etc.. Each analysis result is a key: value pair of function name and polars DataFrame
                # FOR BEST RESULTS, EVERY ANALYSIS SHOULD RETURN A POLARS DATAFRAME. This simplifies the reporting and visualization pipeline.
                analyses: dict[str, pl.DataFrame] = analysis_pipe.run_analysis(data)
                analyses['raw_data'] = data

                # Visualize the data and log to relevant dashboards (e.g. WandB, prefect, and local)
                visualization_pipe.run(data, analyses, session_info_grouped[i])

                # Saving to DuckDB (maybe after Bayesian optimization?)

            # ! Not implemented yet
            # Run Bayesian optimization on the data
            # TODO: Maybe include bayesian optimization in the group loop? This will simplify the reporting, 
            #               but will potentially increase complexity of trying to incorporate multiple observations of reward function simultaneously (one for each group).
            # TODO: Add Bayesian optimization to the pipeline. Each group of sessions will act as invidiual observations of reward function.
            # TODO: Ideally include functionality to incorporate multiple observations simultaneously.

    
    # Add additional information to sessions df. Save QC info in a separate table?
    # sessions = sessions.join(session_process_info, on="SessionName")

    # Add processed sessions to past sessions which were already reported
    reported_sessions_df = reported_sessions_df.vstack(sessions_info.select(pl.all().exclude('Group')))
    
    # Record to csv
    reported_sessions_df.write_csv(global_config["REPORTED_SESSIONS_CSV_PATH"])


@hydra.main(version_base=None, config_path="../configs_and_globals", config_name="config_main")
def hydra_main_pipeline(cfg: DictConfig):
    main_pipeline(cfg)

if __name__ == "__main__":
    hydra_main_pipeline()