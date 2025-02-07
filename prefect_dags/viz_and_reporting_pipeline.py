import polars as pl
import yaml
from typing import Dict, Any, Callable
import importlib

# from configs_and_globals.configs import visualization_config
import src.viz_and_reports.viz_funcs as viz
from src.viz_and_reports.reporting_funcs import log_plotting_result
from prefect import task, flow
import wandb
import os
import warnings
from src.viz_and_reports.reporting_funcs import local_setup
import pandas as pd


def load_function(function_path: str) -> Callable:
    """
    Dynamically load a function from a string path.
    """
    module_name, function_name = function_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, function_name)


class VisualizationAndReportingPipeline:
    def __init__(self, visualization_config):
        self.config = visualization_config
        self.viz_funcs = {
            func_name: getattr(viz, func_name, None)
            for func_name in self.config["functions"]
        }
        self.wandb_config = self.config["wandb"]
        self.local_reporting_config = self.config["local_reporting"]

    # load config from yaml if desired, currently not used
    def load_config(self):
        with open(self.config_path, "r") as file:
            self.config = yaml.safe_load(file)

    def init_wandb(self, session_info: pl.DataFrame, path: str):
        # Add session and device relevant info to wandb_config for filtering in dashboard
        self.wandb_config["tags"] = (
            session_info.get_column("Session#").unique().to_list()
        )
        self.wandb_config["job_type"] = (
            session_info.get_column("Device").unique().to_list()[0]
        )
        self.wandb_config["group"] = (
            session_info.get_column("SessionType(s)").unique().to_list()[0]
        )

        # Initialize wandb
        run = wandb.init(
            project=self.wandb_config["project"],
            entity=self.wandb_config["entity"],
            job_type=self.wandb_config["job_type"],
            group=self.wandb_config["group"],
            tags=self.wandb_config["tags"],
        )

        # Save all session info to wandb
        run.log(
            {
                "session_info": wandb.Table(
                    dataframe=session_info.select(
                        pl.exclude("Data_Server_Hyperlink")
                    # Polars to pandas conversion can mess up timezone info, so explicitly set it
                    ).to_pandas().assign(TimeStarted=lambda df: pd.to_datetime(df['TimeStarted']).dt.tz_localize(self.config["TIMEZONE"]))
                )
            }
        )
        if path:
            run.log({"path_to_local_reports": path})

        # Log settings CSV files for each session in grouping
        tables = {}
        for i in range(session_info.height):
            csv_dir = session_info[i, "settings_path"]
            if os.path.isdir(csv_dir):
                for filename in os.listdir(csv_dir):
                    if filename.endswith(".csv"):
                        csv_path = os.path.join(csv_dir, filename)
                        try:
                            df = pd.read_csv(csv_path)
                            table_name = os.path.splitext(filename)[
                                0
                            ]  # Remove .csv extension
                            if table_name not in tables:
                                tables[table_name] = []
                            tables[table_name].append(df)
                        except Exception as e:
                            warnings.warn(
                                f"Error reading CSV file {csv_path}: {str(e)}"
                            )
            else:
                warnings.warn(f"CSV directory not found: {csv_dir}")

        # Concatenate DataFrames and log as wandb Tables
        for table_name, dfs in tables.items():
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                run.log({table_name: wandb.Table(dataframe=combined_df)})

        return run

    def init_local_reporting(self, session_info):
        # Create local directory structure
        rcs_num = session_info.get_column("Device").unique().to_list()[0]
        session_type = session_info.get_column("SessionType(s)").unique().to_list()[0]
        session_num = "_".join(session_info.get_column("Session#").unique().to_list())
        reporting_path = os.path.join(
            self.local_reporting_config["reporting_path_base"],
            session_type,
            rcs_num,
            "session_reports",
            session_num,
        )
        if not os.path.exists(reporting_path):
            os.makedirs(reporting_path)
        else:
            warnings.warn(f"Directory already exists: {reporting_path}", UserWarning)

        # Code snapshot, git info, and conda package versions saved to local directory
        local_setup(reporting_path, self.local_reporting_config, conda=False)
        return reporting_path

    # @task(name="{func_name}")
    def execute_viz_func(
        self, func_name: str, data: pl.DataFrame, func_params: dict[str, Any] = {}
    ):
        viz_func = self.viz_funcs.get(func_name)
        if viz_func is None:
            print(
                f"Warning: Function '{func_name}' not found in viz_funcs module. Skipping."
            )
            return None

        @task(name=f"viz_func_{func_name}")
        def execute_viz_task(viz_func, data, func_params={}):
            if func_params:
                return viz_func(data, **func_params)
            else:
                return viz_func(data)

        return execute_viz_task(viz_func, data, func_params)

    def run(
        self,
        data_df: pl.DataFrame,
        analyses: Dict[str, Any],
        session_info: pl.DataFrame,
    ):

        # Create local directory structure for logging/reporting locally
        if self.local_reporting_config:
            path = self.init_local_reporting(session_info)
        else:
            path = None

        # Initialize wandb for logging
        if self.wandb_config:
            wandb_run = self.init_wandb(session_info, path)
        else:
            wandb_run = None

        for func_name, func_config in self.config["functions"].items():
            # func_name is the name of the function to execute
            # The first kwarg of the function is the name of the data to plot. The "data" is a key in the analyses dictionary (e.g. "raw_data"). 
            # The value corresponding to the key in the analyses dictionary is the actual data to plot (e.g. a polars dataframe corresponding to the raw data table, or some other processed dataframe).
            # The second kwarg is the log_options, which is a list of options for where to log the plot.
            # The third kwarg is the function-specific kwargs, which is a dictionary of additional keyword arguments to pass to the function

            data_source = func_config["data"]
            log_options = func_config.get("log", [])

            # Get the data from the analyses results
            if data_source in analyses:
                data = analyses[data_source]
            elif (
                data_source == "dataframe"
                or data_source == "data"
                or data_source == "raw_data"
                or data_source == "combinedDataTable"
            ):  # If data source is the raw data
                data = data_df
            else:
                print(
                    f"Warning: Data source '{data_source}' not found in analyses results. Skipping {func_name}."
                )
                continue

            # Execute the visualization function as a Prefect task
            # The .submit() method is used in Prefect to submit a task for asynchronous execution.
            # It schedules the task to run and returns a Future object immediately, allowing for parallel execution.
            # In this case, it's submitting the execute_viz_func task to be run asynchronously with the given parameters.
            if func_params := func_config.get("kwargs", {}):
                result = self.execute_viz_func(func_name, data, func_params)
            else:
                result = self.execute_viz_func(func_name, data)

            log_plotting_result(result, func_name, log_options, wandb_run, path)

        if wandb_run:
            wandb_run.finish()

        print("Visualization pipeline completed successfully.")

        return path


# Example usage:
# pipeline = VisualizationPipeline('path/to/viz_config.yaml')
# pipeline.run(my_dataframe, my_analyses_results)
