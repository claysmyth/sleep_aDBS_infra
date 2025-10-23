"""
Safely add more balanced blocks to an existing A/B test schedule.
Maintains equal numbers of adaptive-first and continuous-first blocks.
"""

import yaml
import random
from datetime import datetime
from pathlib import Path
import sys
import argparse


class BlockAdder:
    def __init__(self, schedule_file='pilot_schedule.yaml'):
        self.schedule_file = Path(schedule_file)
        self.data = None
        
    def load_schedule(self):
        """Load the current schedule."""
        with open(self.schedule_file, 'r') as f:
            self.data = yaml.safe_load(f)
        return self.data
    
    def backup_schedule(self):
        """Create a backup before modifying."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Save backup in same directory as original file
        backup_file = self.schedule_file.parent / f"{self.schedule_file.stem}_backup_{timestamp}.yaml"
        
        with open(self.schedule_file, 'r') as f:
            content = f.read()
        
        with open(backup_file, 'w') as f:
            f.write(content)
            
        print(f"✓ Created backup: {backup_file}")
        return backup_file
    
    def analyze_current_blocks(self):
        """Analyze the current block structure."""
        schedule = self.data['schedule']
        n_nights = len(schedule)
        n_blocks = n_nights // 2
        
        adaptive_first_blocks = 0
        continuous_first_blocks = 0
        
        for i in range(0, n_nights, 2):
            if i + 1 < n_nights:
                if schedule[i] == 0:  # adaptive first
                    adaptive_first_blocks += 1
                else:  # continuous first
                    continuous_first_blocks += 1
        
        return {
            'total_nights': n_nights,
            'total_blocks': n_blocks,
            'adaptive_first': adaptive_first_blocks,
            'continuous_first': continuous_first_blocks,
            'is_balanced': adaptive_first_blocks == continuous_first_blocks
        }
    
    def get_last_seed(self):
        """Get the last used seed from schedule or additions."""
        # Check for seed in schedule additions (most recent first)
        if 'schedule_additions' in self.data and self.data['schedule_additions']:
            for addition in reversed(self.data['schedule_additions']):
                if 'seed' in addition and addition['seed'] is not None:
                    return addition['seed']
        
        # Check for original seed
        if 'seed' in self.data:
            return self.data['seed']
        
        return None
    
    def generate_new_blocks(self, n_blocks_to_add, seed=None):
        """
        Generate new balanced blocks with random order.
        
        Args:
            n_blocks_to_add: Must be even to maintain balance
            seed: Random seed for reproducibility (if None, uses last seed + 1)
        """
        if n_blocks_to_add % 2 != 0:
            raise ValueError("Must add an even number of blocks to maintain balance")
        
        # If no seed provided, try to use last seed + 1 for reproducibility
        if seed is None:
            last_seed = self.get_last_seed()
            if last_seed is not None:
                seed = last_seed + 1
                print(f"  Using seed: {seed} (last seed + 1)")
            else:
                # True random if no seed history found
                print("  Using random seed (no seed history found)")
        else:
            print(f"  Using seed: {seed} (specified)")
        
        if seed is not None:
            random.seed(seed)
        
        # Create half adaptive-first, half continuous-first blocks
        n_each_type = n_blocks_to_add // 2
        new_blocks = []
        
        # Adaptive-first blocks: [0, 1]
        for _ in range(n_each_type):
            new_blocks.append([0, 1])
        
        # Continuous-first blocks: [1, 0]
        for _ in range(n_each_type):
            new_blocks.append([1, 0])
        
        # Randomly shuffle the new blocks
        random.shuffle(new_blocks)
        
        # Flatten to single list
        new_schedule_addition = [treatment for block in new_blocks for treatment in block]
        
        return new_schedule_addition, new_blocks, seed
    
    def add_blocks(self, n_blocks_to_add, seed=None):
        """Add new blocks to the schedule."""
        # Load current schedule
        self.load_schedule()
        
        # Analyze current state
        current_state = self.analyze_current_blocks()
        print("\nCurrent Schedule Analysis:")
        print(f"  Total nights: {current_state['total_nights']}")
        print(f"  Total blocks: {current_state['total_blocks']}")
        print(f"  Adaptive-first blocks: {current_state['adaptive_first']}")
        print(f"  Continuous-first blocks: {current_state['continuous_first']}")
        print(f"  Currently balanced: {current_state['is_balanced']}")
        
        if not current_state['is_balanced']:
            print("\n⚠️  WARNING: Current schedule is not balanced!")
            print("Consider fixing the imbalance before adding more blocks.")
            response = input("Continue anyway? (y/n): ").lower()
            if response != 'y':
                return
        
        # Generate new blocks
        print(f"\nGenerating {n_blocks_to_add} new blocks...")
        new_schedule_addition, new_blocks, used_seed = self.generate_new_blocks(n_blocks_to_add, seed)
        
        # Show what will be added
        print("\nNew blocks to add:")
        for i, block in enumerate(new_blocks):
            treatments = ['adaptive' if t == 0 else 'continuous' for t in block]
            print(f"  Block {current_state['total_blocks'] + i + 1}: [{treatments[0]}, {treatments[1]}]")
        
        # Confirm before proceeding
        print(f"\nThis will extend the schedule from {current_state['total_nights']} to "
              f"{current_state['total_nights'] + len(new_schedule_addition)} nights.")
        response = input("Proceed? (y/n): ").lower()
        
        if response != 'y':
            print("Cancelled.")
            return
        
        # Create backup
        self.backup_schedule()
        
        # Update schedule
        self.data['schedule'].extend(new_schedule_addition)
        self.data['n_nights'] = len(self.data['schedule'])
        
        # Add metadata about the addition
        if 'schedule_additions' not in self.data:
            self.data['schedule_additions'] = []
        
        addition_info = {
            'date': datetime.now().isoformat(),
            'blocks_added': n_blocks_to_add,
            'nights_added': len(new_schedule_addition),
            'seed': used_seed,
            'new_total_nights': self.data['n_nights'],
            'block_structure': [[treatments[0], treatments[1]] for treatments in new_blocks]
        }
        
        self.data['schedule_additions'].append(addition_info)
        
        # Save updated schedule
        with open(self.schedule_file, 'w') as f:
            yaml.dump(self.data, f, default_flow_style=False)
        
        print(f"\n✓ Successfully added {n_blocks_to_add} blocks!")
        print(f"✓ Updated schedule saved to {self.schedule_file}")
        
        # Final analysis
        self.load_schedule()
        final_state = self.analyze_current_blocks()
        print("\nFinal Schedule Analysis:")
        print(f"  Total nights: {final_state['total_nights']}")
        print(f"  Total blocks: {final_state['total_blocks']}")
        print(f"  Adaptive-first blocks: {final_state['adaptive_first']}")
        print(f"  Continuous-first blocks: {final_state['continuous_first']}")
        print(f"  Balanced: {final_state['is_balanced']}")
    
    def show_full_schedule(self):
        """Display the complete schedule with block structure."""
        self.load_schedule()
        schedule = self.data['schedule']
        treatment_map = self.data.get('treatment_map', {0: 'adaptive', 1: 'continuous'})
        
        print("\nComplete Schedule:")
        print("="*50)
        
        for i in range(0, len(schedule), 2):
            block_num = i // 2 + 1
            if i + 1 < len(schedule):
                t1 = treatment_map[schedule[i]]
                t2 = treatment_map[schedule[i+1]]
                print(f"Block {block_num:2d} (nights {i+1:2d}-{i+2:2d}): [{t1:>10}, {t2:>10}]")
            else:
                t1 = treatment_map[schedule[i]]
                print(f"Block {block_num:2d} (night  {i+1:2d}):     [{t1:>10}] (incomplete block!)")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Add balanced blocks to A/B test schedule')
    parser.add_argument('schedule_file', nargs='?', default='pilot_schedule.yaml',
                        help='Path to schedule YAML file (default: pilot_schedule.yaml)')
    parser.add_argument('--show', action='store_true',
                        help='Show full schedule')
    parser.add_argument('--analyze', action='store_true',
                        help='Analyze current balance')
    parser.add_argument('--blocks', type=int,
                        help='Number of blocks to add (must be even)')
    parser.add_argument('--seed', type=int,
                        help='Random seed for reproducibility')
    
    args = parser.parse_args()
    
    # Check if schedule file exists
    schedule_path = Path(args.schedule_file)
    if not schedule_path.exists():
        print(f"Error: Schedule file '{schedule_path}' not found!")
        return
    
    adder = BlockAdder(schedule_path)
    
    if args.show:
        adder.show_full_schedule()
    elif args.analyze:
        adder.load_schedule()
        state = adder.analyze_current_blocks()
        print(f"\nSchedule Analysis for: {schedule_path}")
        for key, value in state.items():
            print(f"  {key}: {value}")
    elif args.blocks:
        # Non-interactive mode with command line arguments
        if args.blocks % 2 != 0:
            print("Error: Must add an even number of blocks!")
            return
        adder.add_blocks(args.blocks, args.seed)
    else:
        # Interactive mode
        print("Add Balanced Blocks to A/B Test Schedule")
        print(f"Schedule file: {schedule_path}")
        print("="*50)
        
        try:
            n_blocks = int(input("\nHow many blocks to add (must be even)? "))
            if n_blocks % 2 != 0:
                print("Error: Must add an even number of blocks!")
                return
                
            seed_input = input("Random seed (press Enter for random): ").strip()
            seed = int(seed_input) if seed_input else None
            
            adder.add_blocks(n_blocks, seed)
            
        except ValueError as e:
            print(f"Error: {e}")
        except KeyboardInterrupt:
            print("\nCancelled.")

if __name__ == "__main__":
    # Usage: python add_blocks_safely.py /path/to/my_schedule.yaml --blocks 4
    main()