"""
Generate randomized treatment schedule for A/B test.
Creates a balanced design with 6 nights of each treatment.
"""

import yaml
import random
from datetime import datetime
from pathlib import Path
import argparse
import sys


def load_config(config_path="pilot_config.yaml"):
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file
    
    Returns:
        Dictionary containing configuration
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"ERROR: Configuration file '{config_path}' not found.")
        print("Please create a pilot_config.yaml file with the required settings.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"ERROR: Invalid YAML in configuration file: {e}")
        sys.exit(1)


def generate_schedule(seed=None, n_nights=12, block_size=2):
    """
    Generate a block-randomized treatment schedule.
    
    Args:
        seed: Random seed for reproducibility
        n_nights: Total number of nights (must be divisible by block_size)
        block_size: Size of each block (default 2)
    
    Returns:
        List of treatment assignments (0 for c1, 1 for c2)
    """
    if n_nights % block_size != 0:
        raise ValueError(f"Number of nights must be divisible by block_size ({block_size})")
    
    if seed is not None:
        random.seed(seed)
    
    n_blocks = n_nights // block_size
    
    # For 2-night blocks: create half [adaptive,continuous] and half [continuous,adaptive]
    if block_size == 2:
        n_blocks_per_type = n_blocks // 2
        blocks = []
        
        # Create blocks with adaptive first
        for _ in range(n_blocks_per_type):
            blocks.append([0, 1])  # [adaptive, continuous]
        
        # Create blocks with continuous first
        for _ in range(n_blocks_per_type):
            blocks.append([1, 0])  # [continuous, adaptive]
        
        # Handle odd number of blocks
        if n_blocks % 2 != 0:
            # Randomly choose which type gets the extra block
            if random.random() < 0.5:
                blocks.append([0, 1])
            else:
                blocks.append([1, 0])
    else:
        raise ValueError("Only block_size=2 is currently supported")
    
    # Randomly shuffle the blocks
    random.shuffle(blocks)
    
    # Flatten blocks into single schedule
    schedule = [treatment for block in blocks for treatment in block]
    
    return schedule


def save_schedule(schedule, seed, c1_paths, c2_paths, dest_paths, filename='pilot_schedule.yaml'):
    """
    Save the schedule and metadata to YAML file.
    """
    data = {
        'created_at': datetime.now().isoformat(),
        'seed': seed,
        'n_nights': len(schedule),
        'block_size': 2,
        'schedule': schedule,
        'treatment_map': {
            0: 'adaptive',
            1: 'continuous'
        },
        'file_paths': {
            'c1_source': [str(p) for p in c1_paths],
            'c2_source': [str(p) for p in c2_paths],
            'destination': [str(p) for p in dest_paths]
        },
        'current_index': 0,
        'completed_nights': [],
        'log': []
    }
    
    with open(filename, 'w') as f:
        yaml.dump(data, f, default_flow_style=False)
    
    print(f"Schedule saved to {filename}")
    print(f"Random seed: {seed}")
    print(f"Schedule: {schedule}")
    print(f"Treatment sequence: {['adaptive' if x == 0 else 'continuous' for x in schedule]}")
    
    # Show block structure
    print(f"\nBlock structure (2-night blocks):")
    for i in range(0, len(schedule), 2):
        block_treatments = ['adaptive' if schedule[i+j] == 0 else 'continuous' for j in range(2)]
        print(f"  Block {i//2 + 1}: [{block_treatments[0]}, {block_treatments[1]}]")


def validate_paths(config):
    """
    Validate that the configured paths exist or can be created.
    
    Args:
        config: Configuration dictionary
    
    Returns:
        Tuple of (c1_paths, c2_paths, dest_paths) as lists of Path objects
    """
    paths = config['paths']
    
    # Convert to lists of Path objects
    c1_paths = [Path(p) for p in paths['c1_source']]
    c2_paths = [Path(p) for p in paths['c2_source']]
    dest_paths = [Path(p) for p in paths['destination']]
    
    # Validate that all lists have the same length
    if len(c1_paths) != len(c2_paths) or len(c1_paths) != len(dest_paths):
        print("ERROR: c1_source, c2_source, and destination must have the same number of files")
        sys.exit(1)
    
    # Check if source files exist
    for i, c1_path in enumerate(c1_paths):
        if not c1_path.exists():
            print(f"WARNING: C1 source file {i+1} not found: {c1_path}")
            print("Please update the path in pilot_config.yaml")
    
    for i, c2_path in enumerate(c2_paths):
        if not c2_path.exists():
            print(f"WARNING: C2 source file {i+1} not found: {c2_path}")
            print("Please update the path in pilot_config.yaml")
    
    # Check if destination directories exist
    for i, dest_path in enumerate(dest_paths):
        dest_dir = dest_path.parent
        if not dest_dir.exists():
            print(f"WARNING: Destination directory {i+1} does not exist: {dest_dir}")
            print("Please create the directory before running the pilot.")
    
    return c1_paths, c2_paths, dest_paths


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate randomized treatment schedule for A/B test')
    parser.add_argument('--config', '-c', default='pilot_config.yaml', 
                       help='Path to configuration file (default: pilot_config.yaml)')
    parser.add_argument('--seed', '-s', type=int, 
                       help='Override random seed from config file')
    parser.add_argument('--n-nights', '-n', type=int, 
                       help='Override number of nights from config file')
    parser.add_argument('--output', '-o', 
                       help='Override output filename from config file')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Get schedule parameters (command line args override config)
    schedule_config = config['schedule']
    seed = args.seed if args.seed is not None else schedule_config['seed']
    n_nights = args.n_nights if args.n_nights is not None else schedule_config['n_nights']
    block_size = schedule_config['block_size']
    output_filename = args.output if args.output else schedule_config['output_filename']
    
    # Validate and get file paths
    c1_paths, c2_paths, dest_paths = validate_paths(config)
    
    print(f"Configuration loaded from: {args.config}")
    print(f"Random seed: {seed}")
    print(f"Number of nights: {n_nights}")
    print(f"Block size: {block_size}")
    print(f"Output file: {output_filename}")
    print(f"Number of files per treatment: {len(c1_paths)}")
    print(f"C1 sources: {[str(p) for p in c1_paths]}")
    print(f"C2 sources: {[str(p) for p in c2_paths]}")
    print(f"Destinations: {[str(p) for p in dest_paths]}")
    print()
    
    # Generate block-randomized schedule
    schedule = generate_schedule(seed=seed, n_nights=n_nights, block_size=block_size)
    
    # Save to file
    save_schedule(
        schedule, 
        seed=seed,
        c1_paths=c1_paths,
        c2_paths=c2_paths,
        dest_paths=dest_paths,
        filename=output_filename
    )
    
    # Print summary statistics
    print(f"\nSummary:")
    print(f"Total nights: {len(schedule)}")
    print(f"Nights with adaptive: {schedule.count(0)}")
    print(f"Nights with continuous: {schedule.count(1)}")


if __name__ == "__main__":
    main()