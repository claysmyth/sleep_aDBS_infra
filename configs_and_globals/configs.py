import yaml
import re

GLOBAL_VARIABLES_PATH = 'configs_and_globals/global_variables.yaml'
ANALYSIS_CONFIG_PATH = 'configs_and_globals/analysis_config.yaml'
VISUALIZATION_CONFIG_PATH = 'configs_and_globals/viz_and_reporting_config.yaml'


class Config:
    configs = {}  # Class variable to store all config instances

    def __init__(self, file_path, name):
        with open(file_path, 'r') as file:
            self.data = yaml.safe_load(file)
        self.name = name
        Config.configs[name] = self
        self._format_strings()
    
    def _format_strings(self):
        def resolve(value):
            if isinstance(value, str):
                # Check for ${...} pattern
                value = re.sub(r'\$\{(.+?)\}', lambda m: self._get_nested_value(m.group(1)), value)
                # Check for {other_config.keyword} pattern
                value = re.sub(r'\{(\w+)\.(\w+)\}', self._get_other_config_value, value)
            elif isinstance(value, list):
                value = [resolve(item) for item in value]
            elif isinstance(value, dict):
                value = {k: resolve(v) for k, v in value.items()}
            return value

        self.data = resolve(self.data)
    
    def _get_nested_value(self, key_path):
        keys = key_path.split('.')
        value = self.data
        for key in keys:
            if key in value:
                value = value[key]
            else:
                return f'${{{key_path}}}'  # Return original placeholder if key not found
        return str(value)
    
    def _get_other_config_value(self, match):
        config_name, key = match.groups()
        if config_name in Config.configs and key in Config.configs[config_name].data:
            return str(Config.configs[config_name].data[key])
        return match.group(0)  # Return original pattern if not found
    
    def to_dict(self):
        return self.data

# Create Config instances and load the YAML files
global_config = Config(GLOBAL_VARIABLES_PATH, 'global').to_dict()
analysis_config = Config(ANALYSIS_CONFIG_PATH, 'analysis').to_dict()
visualization_config = Config(VISUALIZATION_CONFIG_PATH, 'visualization').to_dict()

# Make global_config, analysis_config, and visualization_config importable
__all__ = ['global_config', 'analysis_config', 'visualization_config']


