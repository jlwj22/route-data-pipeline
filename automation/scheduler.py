import schedule
import time
import threading
import logging
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional
import json
from pathlib import Path

from config.settings import Settings
from utils.logger import get_logger

logger = get_logger(__name__)

class TaskScheduler:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.scheduled_jobs: Dict[str, schedule.Job] = {}
        self.running = False
        self.scheduler_thread: Optional[threading.Thread] = None
        self.job_registry: Dict[str, Callable] = {}
        
    def register_job(self, job_name: str, job_function: Callable) -> None:
        """Register a job function that can be scheduled"""
        self.job_registry[job_name] = job_function
        logger.info(f"Registered job: {job_name}")
    
    def schedule_job(self, job_name: str, schedule_config: Dict) -> bool:
        """Schedule a job based on configuration"""
        if job_name not in self.job_registry:
            logger.error(f"Job {job_name} not found in registry")
            return False
            
        job_function = self.job_registry[job_name]
        
        try:
            # Parse schedule configuration
            schedule_type = schedule_config.get('type', 'interval')
            
            if schedule_type == 'interval':
                interval = schedule_config.get('interval', 60)
                unit = schedule_config.get('unit', 'minutes')
                
                if unit == 'seconds':
                    job = schedule.every(interval).seconds.do(self._run_job, job_name, job_function)
                elif unit == 'minutes':
                    job = schedule.every(interval).minutes.do(self._run_job, job_name, job_function)
                elif unit == 'hours':
                    job = schedule.every(interval).hours.do(self._run_job, job_name, job_function)
                elif unit == 'days':
                    job = schedule.every(interval).days.do(self._run_job, job_name, job_function)
                else:
                    logger.error(f"Invalid time unit: {unit}")
                    return False
                    
            elif schedule_type == 'daily':
                time_str = schedule_config.get('time', '09:00')
                job = schedule.every().day.at(time_str).do(self._run_job, job_name, job_function)
                
            elif schedule_type == 'weekly':
                day = schedule_config.get('day', 'monday')
                time_str = schedule_config.get('time', '09:00')
                job = getattr(schedule.every(), day).at(time_str).do(self._run_job, job_name, job_function)
                
            elif schedule_type == 'cron':
                # For cron-like scheduling, we'll use a simple approach
                # In a production system, you might want to use a proper cron library
                logger.warning("Cron scheduling not fully implemented, using daily at 09:00")
                job = schedule.every().day.at('09:00').do(self._run_job, job_name, job_function)
                
            else:
                logger.error(f"Invalid schedule type: {schedule_type}")
                return False
                
            self.scheduled_jobs[job_name] = job
            logger.info(f"Scheduled job: {job_name} with config: {schedule_config}")
            return True
            
        except Exception as e:
            logger.error(f"Error scheduling job {job_name}: {str(e)}")
            return False
    
    def _run_job(self, job_name: str, job_function: Callable) -> None:
        """Execute a scheduled job with error handling"""
        try:
            logger.info(f"Starting scheduled job: {job_name}")
            start_time = datetime.now()
            
            # Execute the job
            result = job_function()
            
            duration = datetime.now() - start_time
            logger.info(f"Completed job: {job_name} in {duration.total_seconds():.2f} seconds")
            
            # Log job execution
            self._log_job_execution(job_name, True, duration, result)
            
        except Exception as e:
            logger.error(f"Error in scheduled job {job_name}: {str(e)}")
            self._log_job_execution(job_name, False, None, str(e))
    
    def _log_job_execution(self, job_name: str, success: bool, duration: Optional[timedelta], result: str) -> None:
        """Log job execution details"""
        log_entry = {
            'job_name': job_name,
            'timestamp': datetime.now().isoformat(),
            'success': success,
            'duration_seconds': duration.total_seconds() if duration else None,
            'result': result
        }
        
        # You could store this in a database or file for monitoring
        logger.info(f"Job execution log: {json.dumps(log_entry)}")
    
    def start_scheduler(self) -> None:
        """Start the scheduler in a separate thread"""
        if self.running:
            logger.warning("Scheduler is already running")
            return
            
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        logger.info("Scheduler started")
    
    def _run_scheduler(self) -> None:
        """Main scheduler loop"""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)  # Check every second
            except Exception as e:
                logger.error(f"Scheduler error: {str(e)}")
                time.sleep(5)  # Wait before retrying
    
    def stop_scheduler(self) -> None:
        """Stop the scheduler"""
        if not self.running:
            logger.warning("Scheduler is not running")
            return
            
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=10)
        
        logger.info("Scheduler stopped")
    
    def unschedule_job(self, job_name: str) -> bool:
        """Remove a scheduled job"""
        if job_name in self.scheduled_jobs:
            schedule.cancel_job(self.scheduled_jobs[job_name])
            del self.scheduled_jobs[job_name]
            logger.info(f"Unscheduled job: {job_name}")
            return True
        else:
            logger.warning(f"Job {job_name} not found in scheduled jobs")
            return False
    
    def list_scheduled_jobs(self) -> List[Dict]:
        """List all scheduled jobs"""
        jobs = []
        for job_name, job in self.scheduled_jobs.items():
            jobs.append({
                'name': job_name,
                'next_run': job.next_run.isoformat() if job.next_run else None,
                'last_run': job.last_run.isoformat() if job.last_run else None
            })
        return jobs
    
    def get_job_status(self, job_name: str) -> Optional[Dict]:
        """Get status of a specific job"""
        if job_name in self.scheduled_jobs:
            job = self.scheduled_jobs[job_name]
            return {
                'name': job_name,
                'next_run': job.next_run.isoformat() if job.next_run else None,
                'last_run': job.last_run.isoformat() if job.last_run else None,
                'run_count': getattr(job, 'run_count', 0)
            }
        return None

class AutomationManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.scheduler = TaskScheduler(settings)
        self.setup_default_jobs()
    
    def setup_default_jobs(self) -> None:
        """Setup default scheduled jobs"""
        # Import here to avoid circular imports
        from data_collection.collection_manager import CollectionManager
        from data_processing.pipeline import DataProcessingPipeline
        from reporting.report_manager import ReportManager
        from automation.backup_manager import BackupManager
        from automation.system_monitor import SystemMonitor
        
        # Initialize database operations
        from database.operations import DatabaseOperations
        db_operations = DatabaseOperations(self.settings.database_path)
        
        # Initialize managers
        collection_config = {
            'max_concurrent_collectors': self.settings.max_concurrent_processes,
            'enable_validation': True,
            'auto_save_to_database': True,
            'collectors': {},
            'validators': {}
        }
        collection_manager = CollectionManager(collection_config, db_operations)
        data_pipeline = DataProcessingPipeline(self.settings, db_operations)
        report_manager = ReportManager(self.settings, db_operations)
        backup_manager = BackupManager(self.settings)
        system_monitor = SystemMonitor(self.settings)
        
        # Register jobs
        self.scheduler.register_job('collect_data', lambda: collection_manager.collect_from_all(parallel=True))
        self.scheduler.register_job('process_data', lambda: data_pipeline.process_raw_data([], enable_geocoding=True, enable_calculations=True))
        self.scheduler.register_job('generate_reports', lambda: report_manager.generate_daily_report(datetime.now()))
        self.scheduler.register_job('backup_database', backup_manager.backup_database)
        self.scheduler.register_job('system_health_check', system_monitor.health_check)
        
        # Load schedule configuration
        self.load_schedule_config()
    
    def load_schedule_config(self) -> None:
        """Load schedule configuration from file"""
        config_file = Path(self.settings.config_dir) / 'automation_schedule.json'
        
        if not config_file.exists():
            # Create default schedule configuration
            default_config = {
                'collect_data': {
                    'type': 'interval',
                    'interval': 30,
                    'unit': 'minutes',
                    'enabled': True
                },
                'process_data': {
                    'type': 'interval',
                    'interval': 1,
                    'unit': 'hours',
                    'enabled': True
                },
                'generate_reports': {
                    'type': 'daily',
                    'time': '08:00',
                    'enabled': True
                },
                'backup_database': {
                    'type': 'daily',
                    'time': '02:00',
                    'enabled': True
                },
                'system_health_check': {
                    'type': 'interval',
                    'interval': 15,
                    'unit': 'minutes',
                    'enabled': True
                }
            }
            
            with open(config_file, 'w') as f:
                json.dump(default_config, f, indent=2)
            
            logger.info(f"Created default schedule configuration: {config_file}")
        
        # Load and apply configuration
        try:
            with open(config_file, 'r') as f:
                schedule_config = json.load(f)
            
            for job_name, job_config in schedule_config.items():
                if job_config.get('enabled', True):
                    self.scheduler.schedule_job(job_name, job_config)
            
            logger.info(f"Loaded schedule configuration from: {config_file}")
            
        except Exception as e:
            logger.error(f"Error loading schedule configuration: {str(e)}")
    
    def start_automation(self) -> None:
        """Start the automation system"""
        self.scheduler.start_scheduler()
        logger.info("Automation system started")
    
    def stop_automation(self) -> None:
        """Stop the automation system"""
        self.scheduler.stop_scheduler()
        logger.info("Automation system stopped")
    
    def get_automation_status(self) -> Dict:
        """Get current automation status"""
        return {
            'scheduler_running': self.scheduler.running,
            'scheduled_jobs': self.scheduler.list_scheduled_jobs(),
            'total_jobs': len(self.scheduler.scheduled_jobs)
        }