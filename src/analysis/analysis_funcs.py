import polars as pl
from configs_and_globals.configs import analysis_config
from prefect import task


@task
def analyze(data):
    # Use config to define function pipeline? Or code within script?
    pass