# Pilot Runner

This directory contains scripts for running A/B test pilots with randomized treatment schedules.

## Files

- `generate_schedule.py` - Generate a randomized treatment schedule
- `run_pilot.py` - Execute daily treatment delivery
- `pilot_config.yaml` - Configuration file for paths and settings
- `pilot_schedule.yaml` - Generated schedule file (created by generate_schedule.py)
- `generate_participant_schedules.py` - Helper script to generate schedules for multiple participants

## Setup

1. **Set global config (optional)**: Edit `run_pilot.py` and set the `GLOBAL_CONFIG_FILE` variable:
   ```python
   # In run_pilot.py, around line 18
   GLOBAL_CONFIG_FILE = "/path/to/your/default_config.yaml"
   
   # For Windows paths, use forward slashes or raw strings:
   GLOBAL_CONFIG_FILE = "C:/Users/RCS16/path/to/config.yaml"  # Use forward slashes
   GLOBAL_CONFIG_FILE = r"C:\Users\RCS16\path\to\config.yaml"  # Or use raw string (r"")
   ```

2. **Configure paths**: Edit `pilot_config.yaml` and update the file paths:
   ```yaml
   paths:
     # Single file format (legacy)
     c1_source: "/path/to/your/c1/treatment/file"
     c2_source: "/path/to/your/c2/treatment/file"
     destination: "/path/to/destination/file"
     
     # OR multi-file format (recommended)
     c1_source: 
       - "/path/to/your/adaptive/treatment1.file"
       - "/path/to/your/adaptive/treatment2.file"
     c2_source: 
       - "/path/to/your/continuous/treatment1.file"
       - "/path/to/your/continuous/treatment2.file"
     destination: 
       - "/path/to/destination1.file"
       - "/path/to/destination2.file"
   ```

2. **Set email credentials**: Create a `.env` file with your Gmail credentials:
   ```
   GMAIL_USERNAME=your.email@gmail.com
   GMAIL_PASSWORD=your_app_password
   ```

3. **Generate schedule**: Run the schedule generation script:
   ```bash
   python generate_schedule.py
   ```

## Usage

### Generate Schedule

```bash
# Use default config
python generate_schedule.py

# Use custom config file
python generate_schedule.py --config my_config.yaml

# Override parameters
python generate_schedule.py --seed 123 --n-nights 20 --output my_schedule.yaml

# Generate schedules for multiple participants (IMPORTANT!)
python generate_participant_schedules.py --participants P001 P002 P003
```

### Run Daily Treatment

```bash
# Use global config setting (set GLOBAL_CONFIG_FILE in run_pilot.py)
python run_pilot.py

# Override with custom config
python run_pilot.py --config my_config.yaml

# Use custom config and schedule
python run_pilot.py --config my_config.yaml --schedule my_schedule.yaml

# Check status only
python run_pilot.py --status
```

## Configuration Options

### Schedule Generation
- `seed`: Random seed for reproducibility
- `n_nights`: Total number of nights
- `block_size`: Size of each block (currently only 2 is supported)
- `output_filename`: Name of the generated schedule file

### Email Settings
- `addresses`: List of email addresses to receive notifications

### File Paths
- `c1_source`: Path(s) to adaptive treatment file(s) (can be single string or list)
- `c2_source`: Path(s) to continuous treatment file(s) (can be single string or list)
- `destination`: Path(s) where treatment files will be copied (must match number of source files)

## Command Line Options

### generate_schedule.py
- `--config, -c`: Path to configuration file
- `--seed, -s`: Override random seed
- `--n-nights, -n`: Override number of nights
- `--output, -o`: Override output filename

### generate_participant_schedules.py
- `--participants, -p`: List of participant IDs (required)
- `--config, -c`: Path to configuration file
- `--base-seed, -s`: Base seed to start from
- `--output-dir, -o`: Directory to save participant schedules

### run_pilot.py
- `--config, -c`: Path to configuration file (overrides global setting)
- `--schedule, -s`: Path to schedule file
- `--status`: Show current study status only

## Randomization for Multiple Participants

**⚠️ CRITICAL**: Each participant must have a different random seed to ensure unique treatment orders.

### Method 1: Manual Seed Assignment
```bash
# Participant 1
python generate_schedule.py --seed 101 --output pilot_schedule_P001.yaml

# Participant 2
python generate_schedule.py --seed 202 --output pilot_schedule_P002.yaml

# Participant 3
python generate_schedule.py --seed 303 --output pilot_schedule_P003.yaml
```

### Method 2: Automated Multi-Participant Generation (Recommended)
```bash
# Generate schedules for multiple participants at once
python generate_participant_schedules.py --participants P001 P002 P003 P004

# This creates:
# schedules/pilot_schedule_P001.yaml
# schedules/pilot_schedule_P002.yaml
# schedules/pilot_schedule_P003.yaml
# schedules/pilot_schedule_P004.yaml
```

### Method 3: Participant-Specific Config Files
Create separate config files for each participant with different seeds:
```yaml
# participant1_config.yaml
schedule:
  seed: 101
  n_nights: 12
  # ... other settings

# participant2_config.yaml
schedule:
  seed: 202
  n_nights: 12
  # ... other settings
```

## Example Workflow

1. **Initial setup for multiple participants**:
   ```bash
   # Generate schedules for all participants
   python generate_participant_schedules.py --participants P001 P002 P003
   
   # Copy appropriate schedule to each participant's device
   cp schedules/pilot_schedule_P001.yaml participant1_device/
   cp schedules/pilot_schedule_P002.yaml participant2_device/
   cp schedules/pilot_schedule_P003.yaml participant3_device/
   ```

2. **Daily execution** (on each participant's device):
   ```bash
   # Run treatment delivery
   python run_pilot.py --schedule pilot_schedule_P001.yaml
   ```

3. **Check progress**:
   ```bash
   # View current status
   python run_pilot.py --status
   ```

## Notes

- The system uses block randomization with 2-night blocks
- Each block contains one night of each treatment (adaptive and continuous)
- Treatment delivery is logged and emailed to specified addresses
- The schedule file tracks progress and can be resumed if interrupted
- **Multi-file support**: You can specify multiple files for each treatment, which will be copied to corresponding destination files
- **Backward compatibility**: The system supports both single-file (legacy) and multi-file formats
- **Global config**: Set `GLOBAL_CONFIG_FILE` in `run_pilot.py` for a default config, or use `--config` to override
- **⚠️ IMPORTANT**: Use different random seeds for different participants to ensure unique treatment orders 