import yaml

GLOBAL_VARIABLES_PATH = 'configs_and_globals/global_variables.yaml'
ANALYSIS_CONFIG_PATH = 'configs_and_globals/analysis_config.yaml'
VISUALIZATION_CONFIG_PATH = 'configs_and_globals/visualization_config.yaml'


class Config:
    def __init__(self, file_path):
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
            for key, value in data.items():
                setattr(self, key, value)
    
    def to_dict(self):
        return {key: getattr(self, key) for key in self.__dict__.keys()}

# Create a Config instance and load the goobal_variables YAML file, so that the variables can be accessed as attributes by other scripts
global_config = Config(GLOBAL_VARIABLES_PATH)

# Repeat but for analysis configurations (e.g. fourier transform params, etc.)
analysis_config = Config(ANALYSIS_CONFIG_PATH)

# Repeat but for visualization configurations (e.g. plotting analyses to wandb, prefect, etc.)
visualization_config = Config(VISUALIZATION_CONFIG_PATH)


