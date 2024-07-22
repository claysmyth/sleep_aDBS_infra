import importlib
import yaml
from prefect import task, flow
from configs_and_globals.configs import analysis_config
from analysis_funcs import *

# Define a function to dynamically load functions
def load_function(module_name, function_name):
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)
    return function


def load_analysis_funcs(config):
    # Define tasks
    tasks = {}
    for step in config['steps']:
        function = load_function(step['module'], step['function'])
        
        # Create Prefect tasks
        @task(name=step['name'])
        def wrapper_function(data, function=function, params=step['params']):
            return function(data, **params)
        
        tasks[step['name']] = wrapper_function
    
    return tasks


@flow
def run_analyses(data, tasks):
    # Run reach task sequentially. Save the output of each analysis in a dictionary
    analyses = {}
    for name, task_function in tasks.items():
        analyses[name] = task_function(data)
    return analyses


def analysis_pipeline(data):
    
    # Load the analysis functions from analysis_config
    tasks = load_analysis_funcs(analysis_config.to_dict())
    
    return run_analyses(data, tasks)
