import polars as pl
import yaml
from typing import Dict, Any, Callable
import importlib
import src.viz_and_reports.viz_funcs as viz
import src.viz_and_reports.reporting_funcs as reporting
from prefect import task, flow

def load_function(function_path: str) -> Callable:
    """
    Dynamically load a function from a string path.
    """
    module_name, function_name = function_path.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, function_name)

class VisualizationPipeline:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.load_config()
        self.viz_funcs = {func_name: getattr(viz, func_name, None) for func_name in self.config['functions']}

    def load_config(self):
        with open(self.config_path, 'r') as file:
            self.config = yaml.safe_load(file)

    @task(name="{func_name}")
    def execute_viz_func(self, func_name: str, data: Any, **kwargs):
        viz_func = self.viz_funcs.get(func_name)
        if viz_func is None:
            print(f"Warning: Function '{func_name}' not found in viz_funcs module. Skipping.")
            return None
        return viz_func(data, **kwargs)

    @flow
    def run(self, data_df: pl.DataFrame, analyses: Dict[str, Any]):

        # TODO: Initialize a new wandb run here?
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

            # TODO: Include artifact logging here, or elsewhere?
            if "WandB" in log_options:
                # Implement WandB logging here
                pass
            if "file" in log_options:
                # Implement file logging here
                pass
            if "prefect" in log_options:
                # Implement Prefect logging here
                pass

        print("Visualization pipeline completed successfully.")

# Example usage:
# pipeline = VisualizationPipeline('path/to/viz_config.yaml')
# pipeline.run(my_dataframe, my_analyses_results)