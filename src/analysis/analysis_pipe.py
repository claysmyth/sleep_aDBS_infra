import importlib
import yaml
from prefect import task, flow
from configs_and_globals.configs import analysis_config
import src.analysis.analysis_funcs as analysis_funcs
import src.analysis.aggregation_criteria as aggregation_criteria
import polars as pl

def load_analysis_funcs(config):
    # Define tasks
    tasks = {}
    for func_name, params in config.items():
        function = getattr(analysis_funcs, func_name)  # Get function from analysis_funcs module
        
        # Create Prefect tasks
        @task(name=func_name)
        def wrapper_function(data, function=function, params=params):
            return function(data, **params)
        
        tasks[func_name] = wrapper_function
    
    return tasks


@flow
def run_analyses(data, tasks):
    # Run reach task sequentially. Save the output of each analysis in a dictionary
    analyses = {}
    for name, task_function in tasks.items():
        analyses[name] = task_function(data)
    return analyses


class AnalysisPipe:
    """
    A class to run analyses on a set of sessions.
    Also handles aggregation criteria and tags.
    """ 
    def __init__(self):
        self.config_dict = analysis_config.to_dict()
        self.tasks = load_analysis_funcs(self.config_dict['functions'])
        self.aggregation_criteria_func = None
        self._setup_aggregation()

    def _setup_aggregation(self):
        if agg_criteria := self.config_dict['aggregation_criteria']:
            func_name = list(agg_criteria.keys())[0]
            kwargs = agg_criteria[func_name]
            if func_name:
                # Aggregation criteria function takes in a polars table of sessions and a list of analysis results corresponding to each session.
                # See src.analysis.aggregation_criteria for more details.
                self.aggregation_criteria_func = lambda sessions, data: getattr(aggregation_criteria, func_name)(sessions, data, **kwargs)
            else:
                raise ValueError("'function_name' key must be specified in aggregation_criteria")
             

    def run_analysis(self, data: pl.DataFrame):
        # Run analyses
        analyses_results = run_analyses(data, self.tasks)
        
        return {
            'analyses_results': analyses_results
        }
    

    def run_aggregation_criteria(self, sessions, analyses):
        return self.aggregation_criteria_func(sessions, analyses)
    
    @task
    def update_data_with_aggregation_criteria(self, sessions_info, sessions_data):
        
        sessions_data_grouped = []
        # Split the sessions info dataframe into groups
        sessions_grouped = sessions_info.with_row_count('SessionIndex').parition_by('Group')

        # For each group, get the relevant data from sessions_data and concatenate it into a single dataframe
        for sessions_group in sessions_grouped:
            relevant_data = sessions_data[sessions_group.get_column('SessionIndex').list()]
            sessions_data_grouped.append(pl.concat(relevant_data, how='vertical').sort('localTime'))

        return sessions_grouped.select(pl.exclude('SessionIndex')), sessions_data_grouped
