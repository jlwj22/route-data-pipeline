#!/usr/bin/env python3
"""
Route Data Pipeline System - Main Entry Point

This is the main execution script for the route data pipeline system.
It provides a command-line interface for running various operations.
"""

import sys
import argparse
import os
from datetime import datetime, timedelta
from typing import Optional

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import settings
from utils.logger import LoggerManager
from database.operations import DatabaseOperations
from utils.helpers import ensure_directory_exists
from data_collection.collection_manager import CollectionManager
from automation.scheduler import AutomationManager
from automation.notification_system import NotificationManager
from automation.backup_manager import BackupManager
from automation.system_monitor import SystemMonitor

class RoutePipelineApp:
    def __init__(self):
        self.settings = settings
        self.logger = None
        self.db = None
        self.collection_manager = None
        self.automation_manager = None
        self.notification_manager = None
        self.backup_manager = None
        self.system_monitor = None
        self.setup_application()
    
    def setup_application(self):
        """Initialize the application"""
        try:
            # Ensure necessary directories exist
            self.settings.ensure_directories()
            
            # Setup logging
            self.logger, self.component_loggers = LoggerManager.setup_application_logging(self.settings)
            
            # Initialize database
            self.db = DatabaseOperations(self.settings.database_path)
            
            # Initialize collection manager
            collection_config = {
                'max_concurrent_collectors': self.settings.max_concurrent_processes,
                'enable_validation': True,
                'auto_save_to_database': True,
                'collectors': {},  # Will be populated from configuration
                'validators': {}
            }
            self.collection_manager = CollectionManager(collection_config, self.db)
            
            # Initialize automation components
            self.automation_manager = AutomationManager(self.settings)
            self.notification_manager = NotificationManager(self.settings)
            self.backup_manager = BackupManager(self.settings)
            self.system_monitor = SystemMonitor(self.settings)
            
            self.logger.info("Route Pipeline System initialized successfully")
            
        except Exception as e:
            print(f"Failed to initialize application: {e}")
            sys.exit(1)
    
    def run_database_setup(self):
        """Initialize or upgrade the database"""
        self.logger.info("Setting up database...")
        
        try:
            # Database is automatically set up in DatabaseOperations constructor
            stats = self.db.get_database_stats()
            self.logger.info(f"Database setup complete. Current stats: {stats}")
            
            print("Database setup completed successfully!")
            for table, count in stats.items():
                print(f"  {table}: {count} records")
                
        except Exception as e:
            self.logger.error(f"Database setup failed: {e}")
            print(f"Database setup failed: {e}")
            sys.exit(1)
    
    def run_data_collection(self, source: str = "all"):
        """Run data collection from specified source"""
        self.logger.info(f"Starting data collection from source: {source}")
        
        try:
            if source == "all":
                # Collect from all enabled sources
                summary = self.collection_manager.collect_from_all(parallel=True)
                
                print(f"Data collection completed:")
                print(f"  Total collectors: {summary.total_collectors}")
                print(f"  Successful: {summary.successful_collections}")
                print(f"  Failed: {summary.failed_collections}")
                print(f"  Records collected: {summary.total_records_collected}")
                print(f"  Errors: {summary.total_errors}")
                print(f"  Warnings: {summary.total_warnings}")
                print(f"  Collection time: {summary.collection_time:.2f}s")
                
                if summary.failed_collections > 0:
                    print("\nFailed collections:")
                    for result in summary.results:
                        if result.status.value == "failed":
                            print(f"  - {result.source_info.get('name', 'Unknown')}: {', '.join(result.errors)}")
            else:
                # Collect from specific source
                if source not in self.collection_manager.collectors:
                    print(f"Error: Collector '{source}' not found")
                    available = list(self.collection_manager.collectors.keys())
                    if available:
                        print(f"Available collectors: {', '.join(available)}")
                    else:
                        print("No collectors configured. Please check configuration.")
                    return
                
                result = self.collection_manager.collect_from_source(source)
                
                print(f"Data collection from '{source}' completed:")
                print(f"  Status: {result.status.value}")
                print(f"  Records collected: {len(result.data)}")
                print(f"  Errors: {len(result.errors)}")
                print(f"  Warnings: {len(result.warnings)}")
                
                if result.errors:
                    print("  Error details:")
                    for error in result.errors:
                        print(f"    - {error}")
                
        except Exception as e:
            self.logger.error(f"Data collection failed: {e}")
            print(f"Data collection failed: {e}")
            sys.exit(1)
    
    def run_data_processing(self, date_range: Optional[str] = None):
        """Run data processing for specified date range"""
        self.logger.info(f"Starting data processing for date range: {date_range}")
        
        try:
            if date_range:
                self.logger.info(f"Data processing would run for date range: {date_range}")
            else:
                self.logger.info("Data processing would run for all unprocessed data")
            
            print("Data processing functionality will be implemented in Phase 3")
            
        except Exception as e:
            self.logger.error(f"Data processing failed: {e}")
            print(f"Data processing failed: {e}")
            sys.exit(1)
    
    def run_report_generation(self, report_type: str = "all", date_range: Optional[str] = None):
        """Generate reports of specified type"""
        self.logger.info(f"Starting report generation - Type: {report_type}, Date range: {date_range}")
        
        try:
            if report_type == "all":
                self.logger.info("Report generation would run for all report types")
            else:
                self.logger.info(f"Report generation would run for type: {report_type}")
            
            print("Report generation functionality will be implemented in Phase 4")
            
        except Exception as e:
            self.logger.error(f"Report generation failed: {e}")
            print(f"Report generation failed: {e}")
            sys.exit(1)
    
    def run_backup(self):
        """Run database backup"""
        self.logger.info("Starting database backup...")
        
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"route_pipeline_backup_{timestamp}.db"
            backup_path = os.path.join(self.settings.backup_path, backup_filename)
            
            ensure_directory_exists(self.settings.backup_path)
            self.db.backup_database(backup_path)
            
            self.logger.info(f"Database backup completed: {backup_path}")
            print(f"Database backup completed: {backup_path}")
            
        except Exception as e:
            self.logger.error(f"Database backup failed: {e}")
            print(f"Database backup failed: {e}")
            sys.exit(1)
    
    def run_vacuum(self):
        """Run database vacuum operation"""
        self.logger.info("Starting database vacuum...")
        
        try:
            self.db.vacuum_database()
            self.logger.info("Database vacuum completed")
            print("Database vacuum completed")
            
        except Exception as e:
            self.logger.error(f"Database vacuum failed: {e}")
            print(f"Database vacuum failed: {e}")
            sys.exit(1)
    
    def show_status(self):
        """Show system status"""
        self.logger.info("Retrieving system status...")
        
        try:
            stats = self.db.get_database_stats()
            collector_status = self.collection_manager.get_collector_status()
            
            print("=== Route Pipeline System Status ===")
            print(f"Database Path: {self.settings.database_path}")
            print(f"Log Level: {self.settings.log_level}")
            print(f"Output Directory: {self.settings.output_directory}")
            
            print("\nDatabase Statistics:")
            for table, count in stats.items():
                print(f"  {table}: {count} records")
            
            print("\nData Collection Status:")
            if collector_status:
                for name, status in collector_status.items():
                    status_icon = "✅" if status['enabled'] and status['connection_status'] else "❌"
                    print(f"  {status_icon} {name} ({status['type']})")
                    print(f"    Enabled: {status['enabled']}")
                    print(f"    Connection: {'OK' if status['connection_status'] else 'Failed'}")
                    print(f"    Collections: {status['collection_count']}")
                    print(f"    Records: {status['total_records_collected']}")
                    if status['last_collection']:
                        print(f"    Last Run: {status['last_collection']}")
            else:
                print("  No collectors configured")
            
            print(f"\nConfiguration File: {self.settings.config_file}")
            print(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve status: {e}")
            print(f"Failed to retrieve status: {e}")
            sys.exit(1)
    
    def run_scheduled_tasks(self):
        """Run scheduled tasks"""
        self.logger.info("Running scheduled tasks...")
        
        try:
            print("Scheduled tasks functionality will be implemented in Phase 5")
            
        except Exception as e:
            self.logger.error(f"Scheduled tasks failed: {e}")
            print(f"Scheduled tasks failed: {e}")
            sys.exit(1)
    
    def start_automation(self):
        """Start the automation system"""
        self.logger.info("Starting automation system...")
        
        try:
            self.automation_manager.start_automation()
            print("Automation system started successfully!")
            print("The system will now run scheduled tasks in the background.")
            print("Press Ctrl+C to stop the automation system.")
            
            # Keep the main thread alive
            import time
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Stopping automation system...")
            self.automation_manager.stop_automation()
            print("\nAutomation system stopped.")
        except Exception as e:
            self.logger.error(f"Automation system failed: {e}")
            print(f"Automation system failed: {e}")
            sys.exit(1)
    
    def stop_automation(self):
        """Stop the automation system"""
        self.logger.info("Stopping automation system...")
        
        try:
            self.automation_manager.stop_automation()
            print("Automation system stopped successfully!")
            
        except Exception as e:
            self.logger.error(f"Failed to stop automation system: {e}")
            print(f"Failed to stop automation system: {e}")
            sys.exit(1)
    
    def show_automation_status(self):
        """Show automation system status"""
        self.logger.info("Retrieving automation status...")
        
        try:
            status = self.automation_manager.get_automation_status()
            
            print("=== Automation System Status ===")
            print(f"Scheduler Running: {'Yes' if status['scheduler_running'] else 'No'}")
            print(f"Total Jobs: {status['total_jobs']}")
            
            if status['scheduled_jobs']:
                print("\nScheduled Jobs:")
                for job in status['scheduled_jobs']:
                    print(f"  - {job['name']}")
                    print(f"    Next Run: {job['next_run'] or 'Not scheduled'}")
                    print(f"    Last Run: {job['last_run'] or 'Never'}")
            else:
                print("\nNo scheduled jobs configured.")
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve automation status: {e}")
            print(f"Failed to retrieve automation status: {e}")
            sys.exit(1)
    
    def run_health_check(self):
        """Run system health check"""
        self.logger.info("Running system health check...")
        
        try:
            health_report = self.system_monitor.health_check()
            
            print("=== System Health Check ===")
            print(f"Overall Status: {health_report['overall_status']}")
            print(f"Timestamp: {health_report['timestamp']}")
            
            print(f"\nSummary:")
            print(f"  Healthy: {health_report['summary']['healthy']}")
            print(f"  Warnings: {health_report['summary']['warnings']}")
            print(f"  Errors: {health_report['summary']['errors']}")
            print(f"  Total Checks: {health_report['summary']['total_checks']}")
            
            print(f"\nComponent Status:")
            for component, details in health_report['components'].items():
                status_icon = "✅" if details['status'] == 'Healthy' else "⚠️" if details['status'] == 'Warning' else "❌"
                print(f"  {status_icon} {component}: {details['status']}")
                print(f"    {details['message']}")
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            print(f"Health check failed: {e}")
            sys.exit(1)
    
    def run_backup_all(self):
        """Run full system backup"""
        self.logger.info("Running full system backup...")
        
        try:
            result = self.backup_manager.full_system_backup()
            
            print("=== Full System Backup ===")
            print(f"Overall Success: {'Yes' if result['success'] else 'No'}")
            print(f"Timestamp: {result['timestamp']}")
            print(f"Successful Backups: {result['successful_backups']}/4")
            print(f"Total Size: {result['total_size']} bytes")
            
            print(f"\nBackup Results:")
            for backup_type, backup_result in result['backup_results'].items():
                status_icon = "✅" if backup_result.get('success', False) else "❌"
                print(f"  {status_icon} {backup_type.title()}")
                if backup_result.get('success'):
                    print(f"    Path: {backup_result.get('backup_path', 'N/A')}")
                    print(f"    Size: {backup_result.get('backup_size', 0)} bytes")
                else:
                    print(f"    Error: {backup_result.get('error', 'Unknown error')}")
            
        except Exception as e:
            self.logger.error(f"Full backup failed: {e}")
            print(f"Full backup failed: {e}")
            sys.exit(1)
    
    def list_backups(self):
        """List all available backups"""
        self.logger.info("Listing available backups...")
        
        try:
            result = self.backup_manager.list_backups()
            
            print("=== Available Backups ===")
            print(f"Total Backups: {result['backup_count']}")
            print(f"Total Size: {result['total_size']} bytes")
            
            if result['backups']:
                print(f"\nBackup Files:")
                for backup in result['backups']:
                    print(f"  - {backup['filename']} ({backup['type']})")
                    print(f"    Size: {backup['size']} bytes")
                    print(f"    Created: {backup['created']}")
                    print(f"    Compressed: {'Yes' if backup['compressed'] else 'No'}")
            else:
                print("\nNo backups found.")
            
        except Exception as e:
            self.logger.error(f"Failed to list backups: {e}")
            print(f"Failed to list backups: {e}")
            sys.exit(1)
    
    def test_notifications(self):
        """Test the notification system"""
        self.logger.info("Testing notification system...")
        
        try:
            success = self.notification_manager.test_notification_system()
            
            if success:
                print("✅ Notification system test successful!")
                print("Check your email for the test message.")
            else:
                print("❌ Notification system test failed!")
                print("Check the logs for error details.")
            
        except Exception as e:
            self.logger.error(f"Notification test failed: {e}")
            print(f"Notification test failed: {e}")
            sys.exit(1)
    
    def launch_dashboard(self):
        """Launch the web dashboard"""
        self.logger.info("Launching web dashboard...")
        
        try:
            # Check if streamlit is available
            try:
                import streamlit
            except ImportError:
                print("❌ Streamlit is not installed.")
                print("Install it with: pip install streamlit")
                print("Or install optional dependencies with: pip install -e .[dashboard]")
                return
            
            import subprocess
            import sys
            from pathlib import Path
            
            dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
            
            if not dashboard_path.exists():
                print("❌ Dashboard not found. Please ensure dashboard/app.py exists.")
                return
            
            print("🚀 Launching Route Data Pipeline Dashboard...")
            print("The dashboard will open in your default web browser.")
            print("Press Ctrl+C to stop the dashboard server.")
            
            # Launch streamlit
            subprocess.run([
                sys.executable, "-m", "streamlit", "run", 
                str(dashboard_path),
                "--server.headless", "false",
                "--server.port", "8501",
                "--browser.gatherUsageStats", "false"
            ])
            
        except KeyboardInterrupt:
            print("\nDashboard stopped.")
        except Exception as e:
            self.logger.error(f"Dashboard launch failed: {e}")
            print(f"Dashboard launch failed: {e}")
            sys.exit(1)

def create_argument_parser():
    """Create and configure argument parser"""
    parser = argparse.ArgumentParser(
        description='Route Data Pipeline System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s setup              # Initialize database
  %(prog)s status             # Show system status
  %(prog)s collect            # Collect data from all sources
  %(prog)s collect --source api  # Collect data from specific source
  %(prog)s process            # Process all unprocessed data
  %(prog)s report             # Generate all reports
  %(prog)s report --type daily   # Generate specific report type
  %(prog)s backup             # Backup database
  %(prog)s vacuum             # Vacuum database
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Setup command
    subparsers.add_parser('setup', help='Initialize or upgrade database')
    
    # Status command
    subparsers.add_parser('status', help='Show system status')
    
    # Data collection command
    collect_parser = subparsers.add_parser('collect', help='Collect data from sources')
    collect_parser.add_argument('--source', default='all',
                               help='Data source to collect from (use "all" for all sources, or specify collector name)')
    collect_parser.add_argument('--parallel', action='store_true', default=True,
                               help='Run collections in parallel (default: True)')
    collect_parser.add_argument('--list', action='store_true',
                               help='List available collectors')
    
    # Data processing command
    process_parser = subparsers.add_parser('process', help='Process collected data')
    process_parser.add_argument('--date-range', 
                               help='Date range to process (YYYY-MM-DD:YYYY-MM-DD)')
    
    # Report generation command
    report_parser = subparsers.add_parser('report', help='Generate reports')
    report_parser.add_argument('--type', default='all',
                              choices=['all', 'daily', 'driver', 'vehicle', 'customer', 'financial'],
                              help='Report type to generate')
    report_parser.add_argument('--date-range',
                              help='Date range for report (YYYY-MM-DD:YYYY-MM-DD)')
    
    # Backup command
    subparsers.add_parser('backup', help='Backup database')
    
    # Vacuum command
    subparsers.add_parser('vacuum', help='Vacuum database')
    
    # Scheduled tasks command
    subparsers.add_parser('scheduled', help='Run scheduled tasks')
    
    # Automation commands
    automation_parser = subparsers.add_parser('automation', help='Automation system commands')
    automation_subparsers = automation_parser.add_subparsers(dest='automation_command', help='Automation commands')
    
    automation_subparsers.add_parser('start', help='Start the automation system')
    automation_subparsers.add_parser('stop', help='Stop the automation system')
    automation_subparsers.add_parser('status', help='Show automation system status')
    
    # Health check command
    subparsers.add_parser('health', help='Run system health check')
    
    # Backup commands
    backup_parser = subparsers.add_parser('backup-all', help='Run full system backup')
    subparsers.add_parser('list-backups', help='List all available backups')
    
    # Notification commands
    subparsers.add_parser('test-notifications', help='Test the notification system')
    
    # Dashboard command (optional)
    subparsers.add_parser('dashboard', help='Launch web dashboard (requires streamlit)')
    
    return parser

def main():
    """Main entry point"""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Initialize application
    app = RoutePipelineApp()
    
    # Execute command
    try:
        if args.command == 'setup':
            app.run_database_setup()
        elif args.command == 'status':
            app.show_status()
        elif args.command == 'collect':
            if args.list:
                # List available collectors
                collector_status = app.collection_manager.get_collector_status()
                if collector_status:
                    print("Available collectors:")
                    for name, status in collector_status.items():
                        enabled = "enabled" if status['enabled'] else "disabled"
                        print(f"  - {name} ({status['type']}) - {enabled}")
                else:
                    print("No collectors configured")
            else:
                app.run_data_collection(args.source)
        elif args.command == 'process':
            app.run_data_processing(args.date_range)
        elif args.command == 'report':
            app.run_report_generation(args.type, args.date_range)
        elif args.command == 'backup':
            app.run_backup()
        elif args.command == 'vacuum':
            app.run_vacuum()
        elif args.command == 'scheduled':
            app.run_scheduled_tasks()
        elif args.command == 'automation':
            if args.automation_command == 'start':
                app.start_automation()
            elif args.automation_command == 'stop':
                app.stop_automation()
            elif args.automation_command == 'status':
                app.show_automation_status()
            else:
                print("Available automation commands: start, stop, status")
        elif args.command == 'health':
            app.run_health_check()
        elif args.command == 'backup-all':
            app.run_backup_all()
        elif args.command == 'list-backups':
            app.list_backups()
        elif args.command == 'test-notifications':
            app.test_notifications()
        elif args.command == 'dashboard':
            app.launch_dashboard()
        else:
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()