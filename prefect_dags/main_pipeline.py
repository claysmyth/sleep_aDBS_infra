from prefect import flow, task
import yaml
import polars as pl
from process_session_pipeline import process_and_session
import os
from src.analysis.analysis_pipe import AnalysisPipe
from src.visualization.visualization_pipe import VisualizationPipe

GLOBALS_PATH = "configs_and_globals/global_variables.yaml"

def get_globals():
    with open(GLOBALS_PATH, "r") as file:
        globals = yaml.safe_load(file)
    return globals

@task
def get_new_sessions(project_df, reported_sessions_df):
    if reported_sessions_df.is_empty():
        return project_df
    else:
        return project_df.join(reported_sessions_df, on="SessionName", how="anti")
    
# @task
# def add_processed_sessions_to_past_sessions(session, reported_sessions_df):
#     print("Adding session to past sessions...")
#     # add info to session
#     reported_sessions_df = reported_sessions_df.vstack(session)
    
@flow
def main_pipeline():
    globals = get_globals()
    
    # Read in table that contains sessions logged to desired project via (insert github link here)
    project_df = pl.read_csv(globals["PROJECT_CSV_PATH"])
    
    # Read in table that contains sessions which have already been reported     
    # Check if the file exists, if not create an empty CSV
    if os.path.exists(globals["REPORTED_SESSIONS_CSV_PATH"]):
        reported_sessions_df = pl.read_csv(globals["REPORTED_SESSIONS_CSV_PATH"])
    else:
        reported_sessions_df = pl.DataFrame()
        reported_sessions_df.write_csv(globals["REPORTED_SESSIONS_CSV_PATH"])
    
    # Get sessions which have not been reported yet
    sessions_info = get_new_sessions(project_df, reported_sessions_df)
    
    # Could create different AnalysisPipes, VisualizationPipes, and ReportingPipes for different projects or session_types.
    # This would require parameterizing the analysis config and passing it into the AnalysisPipe class (same for visualization and reporting).
        # Could manage AnalysisPipes configs with hydra?
    # Currently, only one AnalysisPipe, VisualizationPipe, and ReportingPipe is used for all projects and session types.
    analysis_pipe = AnalysisPipe()
    visualization_pipe = VisualizationPipe()
    
    for sessions_subset_info in sessions_info.partition(['RCS#', 'Side', 'SessionType(s)']):


        sessions_data = []
        for session_info in sessions_subset_info:
            session_data = process_and_session(session_info)
            sessions_data.append(session_data)
        
        # Run aggregation criteria on sessions... otherwise analyze, viz, and bayesian optimize each session individually.
        if len(sessions_subset_info) > 1:
            # Check if sessions are from the same 'unit' or 'group' of analysis (e.g. multiple recordings in a single night of sleep for sleep project)
            sessions_subset_info = analysis_pipe.run_aggregation_criteria(sessions_subset_info, sessions_data)

            list_of_groups, group_data = analysis_pipe.update_data_with_aggregation_criteria(sessions_data)

        else:
            group_data = sessions_data

        # Next, run analysis on the data. Get delta power, etc.. Each analysis result is a key: value pair of function name and polars DataFrame
        for data in group_data:
            # Next, run analysis on the data. Get delta power, etc.. Each analysis result is a key: value pair of function name and polars DataFrame
            analyses: dict[str, pl.DataFrame] = analysis_pipe.run_analysis(data)

            # Visualize the data and log to relevant dashboards (e.g. WandB, prefect, and local)
            visualization_pipe.run(data, analyses)

            # Report the data to relevant dashboards (e.g. WandB, prefect, and local)
            # Likely to be included in the visualization_pipe.run() function

        # Run Bayesian optimization on the data
        # TODO: Maybe include bayesian optimization in the group loop? This will simplify the reporting, 
        #               but will potentially increase complexity of trying to incorporate multiple observations of reward function simultaneously (one for each group).
        # TODO: Add Bayesian optimization to the pipeline. Each group of sessions will act as invidiual observations of reward function.
        # TODO: Ideally include functionality to incorporate multiple observations simultaneously.

    
    # Add additional information to sessions df. Save QC info in a separate table?
    sessions = sessions.join(session_process_info, on="SessionName")

    # Add processed sessions to past sessions which were already reported
    reported_sessions_df = reported_sessions_df.vstack(sessions)
    
    # Record to csv
    reported_sessions_df.write_csv(globals["past_sessions_df"], overwrite=True)
    

if __name__ == "__main__":
    main_pipeline()