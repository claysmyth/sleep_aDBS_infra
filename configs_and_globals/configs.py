import yaml

GLOBAL_VARIABLES_PATH = 'configs_and_globals/global_variables.yaml'
ANALYSIS_CONFIG_PATH = 'configs_and_globals/analysis_config.yaml'
VISUALIZATION_CONFIG_PATH = 'configs_and_globals/viz_and_reporting_config.yaml'


class Config:
    def __init__(self, file_path):
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
            for key, value in data.items():
                setattr(self, key, value)
    
    def to_dict(self):
        return {key: getattr(self, key) for key in self.__dict__.keys()}

# Create Config instances and load the YAML files
global_config = Config(GLOBAL_VARIABLES_PATH).to_dict() 
analysis_config = Config(ANALYSIS_CONFIG_PATH).to_dict()
visualization_config = Config(VISUALIZATION_CONFIG_PATH).to_dict()

# Make global_config, analysis_config, and visualization_config importable
__all__ = ['global_config', 'analysis_config', 'visualization_config']


