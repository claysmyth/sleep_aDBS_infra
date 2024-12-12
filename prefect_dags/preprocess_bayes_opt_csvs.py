## This file is used to preprocess the bayes opt csvs. It ingests the sleep_aDBS_reported_sessions and aggregates the necessary info and filepaths into another csv, that is ultimately ingested into the bayes opt pipeline.
# SHOULD RUN AFTER THE MAIN PIPELINE
import polars as pl
from prefect import flow, task
from prefect_dags.main_pipeline import get_new_sessions
import glob


SESSION_CSV_PATH = "/sleep_aDBS_reported_sessions.csv"
SKIPPED_SESSIONS_CSV_PATH = "/skipped_sessions.csv"
PROCESSED_SESSIONS_PATH = "/bayes_opt/"
BAYES_OPT_CSV_PATH = "/bayes_opt_csvs.csv"
SESSION_TYPES = ["bayes_opt"]
OVERRIDE_FLAG = False

def add_processed_parquet_path(df):
   # Get all parquet files in the processed parquet directory
   processed_dirs = glob.glob(PROCESSED_SESSIONS_PATH)
   df = df.with_columns(
      pl.col("Session#").map_elements(lambda x: [f"{PROCESSED_SESSIONS_PATH}/{y}" for y in processed_dirs if x in y]).alias("processed_session_path")
   ) 
   return df


def preprocess_bayes_opt_csvs():
    sessions_df = (
        get_new_sessions(
            pl.read_csv(SESSION_CSV_PATH), pl.read_csv(SKIPPED_SESSIONS_CSV_PATH)
        )
        # Only keep sessions that are of the desired type
        .filter(pl.col("SessionType").is_in(SESSION_TYPES))
    )
    sessions_df = add_processed_parquet_path(sessions_df)
    bayes_opt_df = pl.read_csv(BAYES_OPT_CSV_PATH).explode("Session#")

    # TODO: bayes_opt_csv will likely aggregate session from teh same night... need to flatten before comparing to sessions_df
    # Check if all sessions from bayes_opt_df exist in sessions_df
    missing_sessions = bayes_opt_df.join(
        sessions_df, 
        on="Session#", 
        how="anti"
    ) 
    if not missing_sessions.is_empty():
        print("Warning: The following sessions from the bayes_opt_csvs are missing from sleep_aDBS_reported_sessions.csv (after filtering skipped sessions):")
        print(missing_sessions.select("Session#"))
        print("Not updating the bayes_opt_csvs.csv file until discrepancy is resolved... or overruled with bool flag")
        if not OVERRIDE_FLAG:
            raise ValueError("The following sessions from the bayes_opt_csvs are missing from sleep_aDBS_reported_sessions.csv (after filtering skipped sessions):")

    # Get sessions from sessions_df that are in the bayes_opt_df
    sessions_in_bayes_opt = sessions_df.join(
        bayes_opt_df, 
        on="Session#", 
        how="inner"
    )

    sessions_in_bayes_opt = add_processed_parquet_path(sessions_in_bayes_opt)


    # Write the updated sessions_df to the bayes_opt_csvs.csv file
    sessions_df.write_csv(BAYES_OPT_CSV_PATH)


if __name__ == "__main__":
    preprocess_bayes_opt_csvs()