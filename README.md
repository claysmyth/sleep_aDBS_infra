# Sleep aDBS Infrastructure

Infrastructure to run sleep adaptive DBS trial. For members of the Little Starr (and adjacent) labs.

## Description

This repository contains a modular, configuration-driven pipeline for processing and analyzing sleep-related adaptive Deep Brain Stimulation (aDBS) data. The pipeline is built using Prefect for workflow orchestration and Hydra for configuration management. It processes session data through multiple stages: data ingestion, analysis, visualization, and reporting, with results logged both locally and to Weights & Biases (wandb) for experiment tracking.

## Architecture Overview

### Main Pipeline
The main pipeline (`prefect_dags/main_pipeline.py`) orchestrates the entire workflow:
1. Loads configurations via Hydra
2. Processes sessions sequentially or in groups based on aggregation criteria. Aggregation is to combine multiple sessions that belong to the same 'unit' of analysis (e.g. multiple sessions belonging to one night of sleep)
3. Coordinates the analysis and visualization sub-pipelines
4. Handles session tracking and error management

### Sub-Pipelines
The pipeline is divided into modular components:
- **Analysis Pipeline**: Processes raw data through configurable analysis functions
- **Visualization Pipeline**: Creates and logs visualizations
- **Session Management**: Tracks processed/failed sessions for resumability
- **Error Handling**: Logs failures while continuing with remaining sessions

### Configuration-Driven Design
The pipeline uses a hierarchical configuration system that determines:
- Which functions to run
- Function parameters
- Data flow between pipeline components
- Logging behavior
- Session aggregation criteria

Example configuration structure:
```yaml
analysis_config:
  functions:
    function_name:
      param1: value1
      param2: value2
  aggregation_criteria:
    agg_func:
      function_name:
        param1: value1

viz_and_reporting_config:
  functions:
    function_name:
      data: "raw_data"  # Which analysis result to visualize
      log: ["wandb", "local"]  # Where to log the visualization
      kwargs:
        param1: value1
```

## Getting Started

### Prerequisites
- Python 3.8+
- Prefect
- Hydra
- Other dependencies listed in `requirements.txt`

### Installation
1. Clone the repository
```bash
git clone [repository-url]
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Set up Prefect (if using local server)
```bash
prefect server start
```

### Running the Pipeline
1. Configure your analysis in the config files under `configs_and_globals/`
2. Run the main pipeline:
```bash
python prefect_dags/main_pipeline.py
```

## Extending Functionality

The pipeline is designed for easy extension through its configuration-driven architecture:

1. **Add New Analysis Functions**:
   - Add function to `src/analysis/analysis_funcs.py`
   - Add function parameters to analysis config
   ```yaml
   analysis_config:
     functions:
       your_new_function:
         param1: value1
   ```

2. **Add New Visualization Functions**:
   - Add function to `src/visualization/viz.py`
   - Add function configuration to visualization config
   ```yaml
   viz_and_reporting_config:
     functions:
       your_new_viz:
         data: "analysis_result_to_plot"
         log: ["wandb", "local"]
   ```

3. **Add New Aggregation Criteria**:
   - Add function to `src/analysis/aggregation_criteria.py`
   - Update aggregation config section

## Pipeline Components

### Analysis Pipeline
The AnalysisPipe class dynamically loads and executes analysis functions based on configuration:
- Functions are loaded from `analysis_funcs` module
- Each function is wrapped as a Prefect task
- Parameters are passed from configuration

### Visualization Pipeline
The VisualizationAndReportingPipeline handles:
- Dynamic loading of visualization functions
- Data source management
- Result logging to wandb and local storage
- Asynchronous execution of visualization tasks

### Session Management
- Tracks processed sessions in a database
- Enables pipeline resumption after interruption
- Logs failed sessions for debugging

## Configuration Files

### Main Config
- Pipeline-level settings
- Sub-pipeline configurations
- Logging settings
- Session management parameters

### Analysis Config
- Analysis function specifications
- Function parameters
- Aggregation criteria
- Session grouping rules

### Visualization Config
- Visualization function specifications
- Data source mappings
- Logging destinations
- Function-specific parameters

## Logging and Monitoring

The pipeline provides comprehensive logging:
- Session progress tracking
- Error logging
- Analysis results
- Visualizations
- Experiment metadata

Results are logged to:
- Local filesystem
- Weights & Biases (wandb)
- Prefect UI

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

[Your License Here]

## Contact

[Your Contact Information]