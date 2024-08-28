import polars as pl
import yaml
from typing import Dict, Any, Callable
import importlib
from configs_and_globals.configs import visualization_config
import src.viz_and_reports.viz_funcs as viz
import src.viz_and_reports.reporting_funcs as reporting
from prefect import task, flow
import wandb
import os
import warnings
import matplotlib, seaborn, plotly, altair
from src.viz_and_reports.reporting_funcs import local_setup

def load_function(function_path: str) -> Callable:
    """
    Dynamically load a function from a string path.
    """
    module_name, function_name = function_path.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, function_name)

class VisualizationAndReportingPipeline:
    def __init__(self):
        self.config = visualization_config
        self.viz_funcs = {func_name: getattr(viz, func_name, None) for func_name in self.config['functions']}
        self.wandb_config = self.config['wandb']
        self.local_reporting_config = self.config['local_reporting']

    # load config from yaml if desired, currently not used
    def load_config(self):
        with open(self.config_path, 'r') as file:
            self.config = yaml.safe_load(file)


    def init_wandb(self, wandb_config: Dict[str, Any], session_info: pl.DataFrame, local_path: str):
        # Add session and device relevant info to wandb_config for filtering in dashboard
        wandb_config['tags'] = (
            session_info.get_column('SessionIdentity').unique().to_list() +
            session_info.get_column('Session#').unique().to_list()
        )
        wandb_config['job_type'] = session_info.get_column('RCS#').unique().to_list()[0]
        wandb_config['group'] = session_info.get_column('SessionType(s)').unique().to_list()[0]

        # Initialize wandb
        run = wandb.init(
            project=wandb_config['project'],
            entity=wandb_config['entity'],
            job_type=wandb_config['job_type'],
            group=wandb_config['group'],
            tags=wandb_config['tags'],
        )
        
        if local_path:
            wandb.log({"local_path": local_path})

        return run, local_path


    def init_local_reporting(self, local_logging_config: Dict[str, Any], session_info: pl.DataFrame):
        # Create local directory structure
        rcs_num = session_info.get_column('RCS#').unique().to_list()[0]
        side = session_info.get_column('Side').unique().to_list()[0]
        session_type = session_info.get_column('SessionType(s)').unique().to_list()[0]
        session_num = '_'.join(session_info.get_column('Session_num').unique().to_list())
        path = os.path.join(local_logging_config['path'], session_type, rcs_num)
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            warnings.warn(f"Directory already exists: {path}", UserWarning)

        # Code snapshot, git info, and conda package versions saved to local directory
        local_setup(path)
        return path


    @task(name="{func_name}")
    def execute_viz_func(self, func_name: str, data: Any, **kwargs):
        viz_func = self.viz_funcs.get(func_name)
        if viz_func is None:
            print(f"Warning: Function '{func_name}' not found in viz_funcs module. Skipping.")
            return None
        return viz_func(data, **kwargs)
    

    @flow
    def run(self, data_df: pl.DataFrame, analyses: Dict[str, Any], session_info: pl.DataFrame):


        # Create local directory structure for logging/reporting locally
        if self.local_logging_config:
            path = self.init_local_reporting(self.local_logging_config, session_info)
        else:
            path = None
        
        # Initialize wandb for logging
        if self.wandb_config:
            wandb_run =self.init_wandb(self.wandb_config, session_info, local_path)
        else:
            wandb_run = None

        for func_name, func_config in self.config['functions'].items():
            data_source = func_config['data']
            log_options = func_config.get('log', [])
            kwargs = func_config.get('kwargs', {})

            # Get the data from the analyses results
            if data_source in analyses:
                data = analyses[data_source]
            elif data_source == "dataframe":
                data = data_df
            else:
                print(f"Warning: Data source '{data_source}' not found in analyses results. Skipping {func_name}.")
                continue

            # Execute the visualization function as a Prefect task
            result = self.execute_viz_func.submit(func_name, data, **kwargs)

            # Log the result based on the specified options
            # TODO: Clean this up. Consider using a more general approach to logging, 
            # TODO: or decorators to manage the type checking and ancillary logic.
            if result:
                if isinstance(result, (altair.Chart, plotly.graph_objs.Figure)):
                    # For Altair or Plotly plots
                    html = result.to_html()
                    if "WandB" in log_options and wandb_run:
                        wandb_run.log({func_name: wandb.Html(html)})
                    if "file" in log_options and path:
                        with open(os.path.join(path, f"{func_name}.html"), "w") as f:
                            f.write(html)
                    if "prefect" in log_options:
                        self.logger.info(f"Visualization {func_name} created")
                elif isinstance(result, (matplotlib.figure.Figure, seaborn.axisgrid.FacetGrid)):
                    # For Matplotlib or Seaborn plots
                    if "WandB" in log_options and wandb_run:
                        wandb_run.log({func_name: wandb.Image(result)})
                    if "file" in log_options and path:
                        result.savefig(os.path.join(path, f"{func_name}.png"))
                    if "prefect" in log_options:
                        self.logger.info(f"Visualization {func_name} created")
                elif isinstance(result, wandb.Table):
                    # For WandB tables
                    if "WandB" in log_options and wandb_run:
                        wandb_run.log({func_name: result})
                    if "file" in log_options and path:
                        result.to_csv(os.path.join(path, f"{func_name}.csv"))
                    if "prefect" in log_options:
                        self.logger.info(f"Table {func_name} created")
                else:
                    self.logger.warning(f"Unsupported result type for {func_name}: {type(result)}")
        
        if wandb_run:
            wandb_run.finish()

        print("Visualization pipeline completed successfully.")

# Example usage:
# pipeline = VisualizationPipeline('path/to/viz_config.yaml')
# pipeline.run(my_dataframe, my_analyses_results)