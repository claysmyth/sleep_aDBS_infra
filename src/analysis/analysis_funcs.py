import polars as pl
from configs_and_globals.configs import analysis_config
from scipy import signal



# ----- Quality Assurance Analysis Functions ------
def null_percentage(data: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """
    Calculate the percentage of null values for specified columns in a DataFrame.

    Args:
        data (pl.DataFrame): The input DataFrame to analyze.
        columns (list[str]): A list of column names to check for null values.

    Returns:
        pl.DataFrame: A DataFrame containing the percentage of null values for each specified column.
    """
    return data.select(pl.col(columns).null_count() / pl.col(columns).len())


def recording_time(data: pl.DataFrame, time_col: str = "localTime") -> pl.DataFrame:
    """
    Calculate the recording time for specified columns in a DataFrame.
    time_col is assumed to be a column of type pl.Datetime.
    """
    return data.select((pl.col(time_col).max() - pl.col(time_col).min()).dt.total_hours().alias("total_recording_time_hours"))



# ----- Time Domain Analysis Functions ------
def time_in_each_stim_amplitude(data: pl.DataFrame, time_col: str = "localTime", stim_col: str = "stimAmplitude") -> pl.DataFrame:
    """
    Calculate the time in each stim amplitude for specified columns in a DataFrame.
    """
    return data.groupby(stim_col).agg(pl.col(time_col).diff().dt.total_seconds().sum().alias("time_in_each_stim_amplitude"))


# ----- Frequency Domain Analysis Functions ------



# ----- Time-Frequency Domain Analysis Functions ------

# ----- Signal Analysis Functions ------
