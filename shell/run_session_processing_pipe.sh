#!/bin/bash

echo "D=======================================================================" >> /home/claysmyth/cron_debug.log
echo "D=======================================================================" >> /home/claysmyth/cron_debug.log
echo "Script started at $(date)" >> /home/claysmyth/cron_debug.log
echo "Current user: $(whoami)" >> /home/claysmyth/cron_debug.log
echo "Current directory: $(pwd)" >> /home/claysmyth/cron_debug.log
echo "PATH: $PATH" >> /home/claysmyth/cron_debug.log

# Change the current directory to /home/claysmyth/code/sleep_aDBS_infra. This helps with relative paths (but hopefully all relative paths have been converted to absolute paths)
cd /home/claysmyth/code/sleep_aDBS_infra

# Log the new current directory
echo "Changed directory to: $(pwd)" >> /home/claysmyth/cron_debug.log

# Add sleep_aDBS_infra and src to PYTHONPATH, so that imports work
export PYTHONPATH="/home/claysmyth/code/sleep_aDBS_infra:/home/claysmyth/code/sleep_aDBS_infra/src:$PYTHONPATH"

# Print the updated PYTHONPATH
echo "Updated PYTHONPATH: $PYTHONPATH" >> /home/claysmyth/cron_debug.log

echo "Running Script" >> /home/claysmyth/cron_debug.log


# Script to run session processing pipeline
{
    /home/claysmyth/miniconda3/envs/sleep_infra/bin/python /home/claysmyth/code/sleep_aDBS_infra/prefect_dags/main_pipeline.py
} >> /home/claysmyth/cron_debug.log 2>&1

echo "Script ended at $(date)" >> /home/claysmyth/cron_debug.log