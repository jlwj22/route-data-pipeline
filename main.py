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

class RoutePipelineApp:
    def __init__(self):
        self.settings = settings
        self.logger = None
        self.db = None
        self.collection_manager = None
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