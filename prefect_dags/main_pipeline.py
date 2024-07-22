from prefect import flow, task
import yaml
import polars as pl
from process_session_pipeline import process_sessions

GLOBALS_PATH = "configs_and_globals/global_variables.yaml"

def get_globals():
    with open(GLOBALS_PATH, "r") as file:
        globals = yaml.safe_load(file)
    return globals

@task
def get_new_sessions(project_df, reported_sessions_df):
    return project_df.join(reported_sessions_df, on="SessionName", how="anti")
    
# @task
# def add_processed_sessions_to_past_sessions(session, reported_sessions_df):
#     print("Adding session to past sessions...")
#     # add info to session
#     reported_sessions_df = reported_sessions_df.vstack(session)
    
@flow
def main_pipeline():
    globals = get_globals()
    project_df = pl.read_csv(globals["project_df"])
    reported_sessions_df = pl.read_csv(globals["past_sessions_df"])
    sessions = get_new_sessions(project_df, reported_sessions_df)
    for participant in sessions.partition("Participant"):
        # Need to include check that will aggregate sessions which occured within the same night (can pass conditional via a config for generalizability)
        # E.g. checks if two sessions from the same device are within a specific time delta from each other, using the Session Name as the timestamp
        # Could also log important information and figures into WandB (for reduncancy sake, but so I can access history remotely via browswer?)
        session_process_info = process_sessions(participant)
    
    # Add additional information to sessions df
    sessions = sessions.join(session_process_info, on="SessionName")
    # Add processed sessions to past sessions which were already reported
    reported_sessions_df = reported_sessions_df.vstack(sessions)
    # Record to csv
    reported_sessions_df.write_csv(globals["past_sessions_df"], overwrite=True)
    

if __name__ == "__main__":
    main_pipeline()