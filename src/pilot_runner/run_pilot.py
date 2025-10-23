"""
Daily execution script for A/B test pilot.
Delivers appropriate treatment and logs compliance.

CONFIGURATION:
- Set GLOBAL_CONFIG_FILE below to specify your default config file
- Command line --config argument will override the global setting
- Example: GLOBAL_CONFIG_FILE = "/path/to/my_config.yaml"
"""

import yaml
# import sys
import shutil
from datetime import datetime
from pathlib import Path
# import json
import traceback
import yagmail
import os
from dotenv import load_dotenv

# Global configuration - can be modified before running the script
# Command line arguments will override this setting
# For Windows paths, use forward slashes or raw strings:
# GLOBAL_CONFIG_FILE = "C:/Users/RCS16/path/to/config.yaml"  # Use forward slashes
# GLOBAL_CONFIG_FILE = r"C:\Users\RCS16\path\to\config.yaml"  # Or use raw string (r"")
GLOBAL_CONFIG_FILE = "pilot_config.yaml"

# Default configuration (fallback)
DEFAULT_CONFIG_FILE = "pilot_config.yaml"
SCHEDULE_FILE = "pilot_schedule.yaml"


def load_config(config_path=DEFAULT_CONFIG_FILE):
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"WARNING: Configuration file '{config_path}' not found. Using defaults.")
        return {}
    except yaml.YAMLError as e:
        print(f"ERROR: Invalid YAML in configuration file: {e}")
        return {}


def load_gmail_credentials_env():
    load_dotenv()
    username = os.getenv("GMAIL_USERNAME")
    password = os.getenv("GMAIL_PASSWORD")
    if not username or not password:
        raise ValueError("Gmail credentials not found in environment variables")
    return {"username": username, "password": password}


class PilotRunner:
    def __init__(self, schedule_file=SCHEDULE_FILE, config_file=DEFAULT_CONFIG_FILE):
        self.schedule_file = schedule_file
        self.config = load_config(config_file)
        self.data = None
        # Get email addresses from config or use defaults
        self.email_addresses = self.config.get('email', {}).get('addresses', 
            ["clay.smyth@ucsf.edu", "karena.balagula@ucsf.edu"])
        
    def load_schedule(self):
        """Load the schedule from YAML file."""
        try:
            with open(self.schedule_file, 'r') as f:
                self.data = yaml.safe_load(f)
            return True
        except FileNotFoundError:
            print(f"ERROR: Schedule file '{self.schedule_file}' not found.")
            print("Please run generate_schedule.py first.")
            return False
        except Exception as e:
            print(f"ERROR loading schedule: {e}")
            return False
    
    def save_schedule(self):
        """Save updated schedule back to YAML file."""
        with open(self.schedule_file, 'w') as f:
            yaml.dump(self.data, f, default_flow_style=False)
    
    def get_current_treatment(self):
        """Get the treatment for current night."""
        current_idx = self.data['current_index']
        
        if current_idx >= len(self.data['schedule']):
            return None, "All treatments completed"
        
        treatment_code = self.data['schedule'][current_idx]
        treatment_name = self.data['treatment_map'][treatment_code]
        
        return treatment_name, None
    
    def get_block_info(self, night_index):
        """Get block number and position within block."""
        block_number = night_index // 2 + 1
        position_in_block = night_index % 2 + 1
        return block_number, position_in_block
    
    def deliver_treatment(self, treatment):
        """
        Deliver the treatment by copying the appropriate files to destinations.
        """
        # Get file paths
        file_paths = self.data['file_paths']
        
        # Handle both old single-file format and new multi-file format
        if isinstance(file_paths['c1_source'], str):
            # Legacy single-file format
            if treatment == 'adaptive':
                source_paths = [Path(file_paths['c1_source'])]
            else:  # continuous
                source_paths = [Path(file_paths['c2_source'])]
            dest_paths = [Path(file_paths['destination'])]
        else:
            # New multi-file format
            if treatment == 'adaptive':
                source_paths = [Path(p) for p in file_paths['c1_source']]
            else:  # continuous
                source_paths = [Path(p) for p in file_paths['c2_source']]
            dest_paths = [Path(p) for p in file_paths['destination']]
        
        # Verify all source files exist
        for i, source_path in enumerate(source_paths):
            if not source_path.exists():
                raise FileNotFoundError(f"Source file {i+1} not found: {source_path}")
        
        # Create destination directories if needed
        for dest_path in dest_paths:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy all files
        copied_files = []
        try:
            for i, (source_path, dest_path) in enumerate(zip(source_paths, dest_paths)):
                shutil.copy2(source_path, dest_path)
                copied_files.append((source_path, dest_path))
                
                # Verify copy was successful
                if not dest_path.exists():
                    raise Exception(f"Destination file {i+1} not created: {dest_path}")
                
                # Optional: verify file integrity
                source_size = source_path.stat().st_size
                dest_size = dest_path.stat().st_size
                if source_size != dest_size:
                    raise Exception(f"File size mismatch for file {i+1}: {source_size} != {dest_size}")
            
            # Treatment delivered successfully - no terminal output to maintain blinding
            
            return True
            
        except Exception as e:
            print(f"ERROR: Failed to copy files: {e}")
            raise
    
    def log_event(self, event_type, details):
        """Add entry to the log."""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'details': details
        }
        self.data['log'].append(log_entry)
    
    def send_email_notification(self, subject, body, html_body=None):
        """Send email notification about treatment delivery."""
        try:
            # Load Gmail credentials from environment variables
            gmail_creds = load_gmail_credentials_env()
            
            # Initialize yagmail SMTP client with credentials
            yag = yagmail.SMTP(gmail_creds["username"], gmail_creds["password"])
            
            # Send email with contents
            yag.send(
                to=self.email_addresses,
                subject=subject,
                contents=body
            )
            
            return True
            
        except Exception as e:
            print(f"Failed to send email notification: {e}")
            self.log_event('email_failed', {'error': str(e)})
            return False
    
    def send_treatment_email(self, night, treatment, block_num, pos_in_block, 
                            success=True, error=None):
        """Send email about treatment delivery."""
        status = "SUCCESS" if success else "FAILURE"
        subject = f"[A/B Test] Night {night} - {status}"
        
        # Plain text body
        body = f"""A/B Test Pilot - Treatment Delivery Report
        
Night: {night}
Block: {block_num} (position {pos_in_block} of 2)
Treatment: {treatment.upper()}
Status: {status}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        if error:
            body += f"\nError: {error}"
        
        # HTML body for better formatting
        html_body = f"""
        <html>
          <body style="font-family: Arial, sans-serif;">
            <h2>A/B Test Pilot - Treatment Delivery Report</h2>
            <table style="border-collapse: collapse; margin: 20px 0;">
              <tr>
                <td style="padding: 8px; background-color: #f0f0f0;"><strong>Night:</strong></td>
                <td style="padding: 8px;">{night} of {self.data['n_nights']}</td>
              </tr>
              <tr>
                <td style="padding: 8px; background-color: #f0f0f0;"><strong>Block:</strong></td>
                <td style="padding: 8px;">{block_num} (position {pos_in_block} of 2)</td>
              </tr>
              <tr>
                <td style="padding: 8px; background-color: #f0f0f0;"><strong>Treatment:</strong></td>
                <td style="padding: 8px;"><strong style="color: {'#2196F3' if treatment == 'c1' else '#FF9800'};">{treatment.upper()}</strong></td>
              </tr>
              <tr>
                <td style="padding: 8px; background-color: #f0f0f0;"><strong>Status:</strong></td>
                <td style="padding: 8px;"><strong style="color: {'green' if success else 'red'};">{status}</strong></td>
              </tr>
              <tr>
                <td style="padding: 8px; background-color: #f0f0f0;"><strong>Timestamp:</strong></td>
                <td style="padding: 8px;">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td>
              </tr>
              {'<tr><td style="padding: 8px; background-color: #f0f0f0;"><strong>Error:</strong></td><td style="padding: 8px; color: red;">' + error + '</td></tr>' if error else ''}
            </table>
            
            <h3>Progress Summary</h3>
            <p>Completed: {len(self.data['completed_nights'])} / {self.data['n_nights']} nights</p>
            
            <hr style="margin: 20px 0;">
            <p style="font-size: 12px; color: #666;">This is an automated message from the A/B Test Pilot system.</p>
          </body>
        </html>
        """
        
        self.send_email_notification(subject, body, html_body)
    
    def run(self):
        """Main execution logic."""
        # Load schedule
        if not self.load_schedule():
            return False
        
        # Check if study is complete
        treatment, error = self.get_current_treatment()
        if treatment is None:
            print(f"Study complete! {error}")
            self.log_event('study_complete', {'message': error})
            self.save_schedule()
            return True
        
        # Display current night info (without revealing treatment)
        current_idx = self.data['current_index']
        total_nights = self.data['n_nights']
        completed = len(self.data['completed_nights'])
        block_num, pos_in_block = self.get_block_info(current_idx)
        
        print(f"Night {current_idx + 1} of {total_nights}")
        print(f"Block {block_num}, position {pos_in_block} of 2")
        print(f"Completed nights: {completed}")
        print("Proceeding with treatment delivery...")
        
        # Deliver treatment
        try:
            success = self.deliver_treatment(treatment)
            
            if success:
                # Update state
                self.data['completed_nights'].append({
                    'night': current_idx + 1,
                    'treatment': treatment,
                    'delivered_at': datetime.now().isoformat()
                })
                self.data['current_index'] += 1
                
                # Log success
                self.log_event('treatment_delivered', {
                    'night': current_idx + 1,
                    'treatment': treatment,
                    'success': True
                })
                
                print(f"\nTreatment delivered successfully!")
                print(f"Next execution will be night {self.data['current_index'] + 1}")
                
                # Send email notification
                self.send_treatment_email(current_idx + 1, treatment, block_num, 
                                        pos_in_block, success=True)
            else:
                raise Exception("Treatment delivery failed")
                
        except Exception as e:
            # Log failure
            self.log_event('treatment_failed', {
                'night': current_idx + 1,
                'treatment': treatment,
                'error': str(e),
                'traceback': traceback.format_exc()
            })
            print(f"\nERROR: Failed to deliver treatment: {e}")
            # Send failure notification
            self.send_treatment_email(current_idx + 1, treatment, block_num, 
                                    pos_in_block, success=False, error=str(e))
            return False
        
        finally:
            # Always save state
            self.save_schedule()
        
        return True
    
    def get_status(self):
        """Print current study status."""
        if not self.load_schedule():
            return
        
        print("\nSTUDY STATUS")
        print("="*50)
        print(f"Total nights: {self.data['n_nights']}")
        print(f"Current night: {self.data['current_index'] + 1}")
        print(f"Completed nights: {len(self.data['completed_nights'])}")
        print(f"Remaining nights: {self.data['n_nights'] - len(self.data['completed_nights'])}")
        
        # Show current block
        current_idx = self.data['current_index']
        if current_idx < self.data['n_nights']:
            current_block = current_idx // 2 + 1
            position_in_block = current_idx % 2 + 1
            print(f"Current block: {current_block} (night {position_in_block} of 2)")
        
        if self.data['completed_nights']:
            print("\nCompleted treatments:")
            for night in self.data['completed_nights']:
                print(f"  Night {night['night']}: {night['treatment']} "
                      f"(delivered at {night['delivered_at']})")
        
        # Don't reveal upcoming treatments to maintain blinding
        if self.data['current_index'] < self.data['n_nights']:
            print(f"\nNext treatment: [BLINDED]")
        
        # Show file paths
        print("\nFile paths:")
        file_paths = self.data['file_paths']
        
        # Handle both single-file and multi-file formats
        if isinstance(file_paths['c1_source'], str):
            # Single-file format
            print(f"  adaptive source: {file_paths['c1_source']}")
            print(f"  continuous source: {file_paths['c2_source']}")
            print(f"  Destination: {file_paths['destination']}")
        else:
            # Multi-file format
            print(f"  adaptive sources ({len(file_paths['c1_source'])} files):")
            for i, path in enumerate(file_paths['c1_source']):
                print(f"    {i+1}. {path}")
            print(f"  continuous sources ({len(file_paths['c2_source'])} files):")
            for i, path in enumerate(file_paths['c2_source']):
                print(f"    {i+1}. {path}")
            print(f"  Destinations ({len(file_paths['destination'])} files):")
            for i, path in enumerate(file_paths['destination']):
                print(f"    {i+1}. {path}")


def main():
    """Main entry point."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run A/B test pilot treatment delivery')
    parser.add_argument('--config', '-c', 
                       help=f'Path to configuration file (overrides global setting)')
    parser.add_argument('--schedule', '-s', default=SCHEDULE_FILE,
                       help=f'Path to schedule file (default: {SCHEDULE_FILE})')
    parser.add_argument('--status', action='store_true',
                       help='Show current study status')
    
    args = parser.parse_args()
    
    # Determine config file to use (command line takes precedence over global)
    config_file = args.config if args.config is not None else GLOBAL_CONFIG_FILE
    
    print(f"Using config file: {config_file}")
    
    # Initialize runner with config
    runner = PilotRunner(schedule_file=args.schedule, config_file=config_file)
    
    # Check for command line arguments
    if args.status:
        runner.get_status()
    else:
        runner.run()


if __name__ == "__main__":
    main()