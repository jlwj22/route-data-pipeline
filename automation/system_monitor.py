import psutil
import sqlite3
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import json
import time
import threading
from dataclasses import dataclass

from config.settings import Settings
from utils.logger import get_logger

logger = get_logger(__name__)

@dataclass
class HealthCheck:
    component: str
    status: str
    message: str
    timestamp: datetime
    metrics: Dict[str, Any] = None

class SystemMonitor:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.monitoring_enabled = True
        self.health_checks: List[HealthCheck] = []
        self.performance_metrics = {}
        self.alert_thresholds = self.load_alert_thresholds()
        
    def load_alert_thresholds(self) -> Dict[str, Any]:
        """Load alert thresholds from configuration"""
        try:
            config_file = Path(self.settings.config_dir) / 'monitoring_thresholds.json'
            
            if not config_file.exists():
                # Create default thresholds
                default_thresholds = {
                    'cpu_usage_percent': 80,
                    'memory_usage_percent': 85,
                    'disk_usage_percent': 90,
                    'database_size_mb': 1000,
                    'log_file_size_mb': 100,
                    'response_time_seconds': 30,
                    'error_rate_percent': 5
                }
                
                with open(config_file, 'w') as f:
                    json.dump(default_thresholds, f, indent=2)
                
                logger.info(f"Created default monitoring thresholds: {config_file}")
            
            with open(config_file, 'r') as f:
                return json.load(f)
                
        except Exception as e:
            logger.error(f"Error loading alert thresholds: {str(e)}")
            return {}
    
    def check_system_resources(self) -> HealthCheck:
        """Check system CPU, memory, and disk usage"""
        try:
            # Get system metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            metrics = {
                'cpu_usage_percent': cpu_percent,
                'memory_usage_percent': memory.percent,
                'memory_available_gb': memory.available / (1024**3),
                'disk_usage_percent': disk.percent,
                'disk_free_gb': disk.free / (1024**3)
            }
            
            # Check against thresholds
            issues = []
            
            if cpu_percent > self.alert_thresholds.get('cpu_usage_percent', 80):
                issues.append(f"High CPU usage: {cpu_percent:.1f}%")
            
            if memory.percent > self.alert_thresholds.get('memory_usage_percent', 85):
                issues.append(f"High memory usage: {memory.percent:.1f}%")
            
            if disk.percent > self.alert_thresholds.get('disk_usage_percent', 90):
                issues.append(f"High disk usage: {disk.percent:.1f}%")
            
            if issues:
                status = "Warning"
                message = "; ".join(issues)
            else:
                status = "Healthy"
                message = "System resources within normal limits"
            
            return HealthCheck(
                component="System Resources",
                status=status,
                message=message,
                timestamp=datetime.now(),
                metrics=metrics
            )
            
        except Exception as e:
            logger.error(f"Error checking system resources: {str(e)}")
            return HealthCheck(
                component="System Resources",
                status="Error",
                message=f"Failed to check system resources: {str(e)}",
                timestamp=datetime.now()
            )
    
    def check_database_health(self) -> HealthCheck:
        """Check database connectivity and size"""
        try:
            db_path = Path(self.settings.get_value('database', 'path', 'route_data.db'))
            
            if not db_path.exists():
                return HealthCheck(
                    component="Database",
                    status="Error",
                    message="Database file not found",
                    timestamp=datetime.now()
                )
            
            # Check database size
            db_size_mb = db_path.stat().st_size / (1024 * 1024)
            
            # Test database connection
            start_time = time.time()
            with sqlite3.connect(str(db_path)) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                table_count = cursor.fetchone()[0]
                
                # Test a simple query
                cursor.execute("SELECT 1")
                cursor.fetchone()
                
            response_time = time.time() - start_time
            
            metrics = {
                'database_size_mb': db_size_mb,
                'table_count': table_count,
                'response_time_seconds': response_time
            }
            
            # Check against thresholds
            issues = []
            
            if db_size_mb > self.alert_thresholds.get('database_size_mb', 1000):
                issues.append(f"Large database size: {db_size_mb:.1f}MB")
            
            if response_time > self.alert_thresholds.get('response_time_seconds', 30):
                issues.append(f"Slow database response: {response_time:.2f}s")
            
            if issues:
                status = "Warning"
                message = "; ".join(issues)
            else:
                status = "Healthy"
                message = f"Database operational with {table_count} tables ({db_size_mb:.1f}MB)"
            
            return HealthCheck(
                component="Database",
                status=status,
                message=message,
                timestamp=datetime.now(),
                metrics=metrics
            )
            
        except Exception as e:
            logger.error(f"Error checking database health: {str(e)}")
            return HealthCheck(
                component="Database",
                status="Error",
                message=f"Database health check failed: {str(e)}",
                timestamp=datetime.now()
            )
    
    def check_log_files(self) -> HealthCheck:
        """Check log file sizes and recent activity"""
        try:
            logs_dir = Path(self.settings.get_value('logging', 'log_directory', 'logs'))
            
            if not logs_dir.exists():
                return HealthCheck(
                    component="Log Files",
                    status="Warning",
                    message="Log directory not found",
                    timestamp=datetime.now()
                )
            
            total_size_mb = 0
            log_files = []
            large_files = []
            
            for log_file in logs_dir.glob('*.log'):
                file_size_mb = log_file.stat().st_size / (1024 * 1024)
                total_size_mb += file_size_mb
                
                log_files.append({
                    'name': log_file.name,
                    'size_mb': file_size_mb,
                    'modified': datetime.fromtimestamp(log_file.stat().st_mtime)
                })
                
                if file_size_mb > self.alert_thresholds.get('log_file_size_mb', 100):
                    large_files.append(f"{log_file.name} ({file_size_mb:.1f}MB)")
            
            metrics = {
                'total_log_size_mb': total_size_mb,
                'log_file_count': len(log_files),
                'large_files': large_files
            }
            
            if large_files:
                status = "Warning"
                message = f"Large log files detected: {', '.join(large_files)}"
            else:
                status = "Healthy"
                message = f"{len(log_files)} log files totaling {total_size_mb:.1f}MB"
            
            return HealthCheck(
                component="Log Files",
                status=status,
                message=message,
                timestamp=datetime.now(),
                metrics=metrics
            )
            
        except Exception as e:
            logger.error(f"Error checking log files: {str(e)}")
            return HealthCheck(
                component="Log Files",
                status="Error",
                message=f"Log file check failed: {str(e)}",
                timestamp=datetime.now()
            )
    
    def check_data_collection_status(self) -> HealthCheck:
        """Check recent data collection activity"""
        try:
            # This would typically check the database for recent collection activity
            # For now, we'll simulate this check
            
            db_path = Path(self.settings.get_value('database', 'path', 'route_data.db'))
            
            if not db_path.exists():
                return HealthCheck(
                    component="Data Collection",
                    status="Error",
                    message="Cannot check data collection - database not found",
                    timestamp=datetime.now()
                )
            
            with sqlite3.connect(str(db_path)) as conn:
                cursor = conn.cursor()
                
                # Check if routes table exists and has recent data
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='routes'")
                if not cursor.fetchone():
                    return HealthCheck(
                        component="Data Collection",
                        status="Warning",
                        message="Routes table not found - no data collected yet",
                        timestamp=datetime.now()
                    )
                
                # Check for recent data (within last 24 hours)
                cursor.execute("""
                    SELECT COUNT(*) FROM routes 
                    WHERE created_at >= datetime('now', '-1 day')
                """)
                recent_count = cursor.fetchone()[0]
                
                # Check total record count
                cursor.execute("SELECT COUNT(*) FROM routes")
                total_count = cursor.fetchone()[0]
                
                metrics = {
                    'recent_records_24h': recent_count,
                    'total_records': total_count
                }
                
                if recent_count == 0:
                    status = "Warning"
                    message = f"No recent data collection (0 records in last 24h, {total_count} total)"
                else:
                    status = "Healthy"
                    message = f"Data collection active ({recent_count} records in last 24h, {total_count} total)"
                
                return HealthCheck(
                    component="Data Collection",
                    status=status,
                    message=message,
                    timestamp=datetime.now(),
                    metrics=metrics
                )
                
        except Exception as e:
            logger.error(f"Error checking data collection status: {str(e)}")
            return HealthCheck(
                component="Data Collection",
                status="Error",
                message=f"Data collection check failed: {str(e)}",
                timestamp=datetime.now()
            )
    
    def check_report_generation(self) -> HealthCheck:
        """Check report generation status"""
        try:
            reports_dir = Path(self.settings.get_value('reports', 'output_directory', 'data/output'))
            
            if not reports_dir.exists():
                return HealthCheck(
                    component="Report Generation",
                    status="Warning",
                    message="Reports directory not found",
                    timestamp=datetime.now()
                )
            
            # Check for recent reports
            recent_reports = []
            total_reports = 0
            cutoff_time = datetime.now() - timedelta(days=7)
            
            for report_file in reports_dir.glob('*.xlsx'):
                file_time = datetime.fromtimestamp(report_file.stat().st_mtime)
                total_reports += 1
                
                if file_time > cutoff_time:
                    recent_reports.append({
                        'name': report_file.name,
                        'created': file_time,
                        'size_mb': report_file.stat().st_size / (1024 * 1024)
                    })
            
            metrics = {
                'recent_reports_7d': len(recent_reports),
                'total_reports': total_reports
            }
            
            if not recent_reports:
                status = "Warning"
                message = f"No recent reports generated (0 in last 7 days, {total_reports} total)"
            else:
                status = "Healthy"
                message = f"Report generation active ({len(recent_reports)} in last 7 days, {total_reports} total)"
            
            return HealthCheck(
                component="Report Generation",
                status=status,
                message=message,
                timestamp=datetime.now(),
                metrics=metrics
            )
            
        except Exception as e:
            logger.error(f"Error checking report generation: {str(e)}")
            return HealthCheck(
                component="Report Generation",
                status="Error",
                message=f"Report generation check failed: {str(e)}",
                timestamp=datetime.now()
            )
    
    def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        try:
            logger.info("Starting system health check")
            
            # Clear previous health checks
            self.health_checks.clear()
            
            # Perform all health checks
            checks = [
                self.check_system_resources(),
                self.check_database_health(),
                self.check_log_files(),
                self.check_data_collection_status(),
                self.check_report_generation()
            ]
            
            self.health_checks.extend(checks)
            
            # Calculate overall health status
            error_count = sum(1 for check in checks if check.status == "Error")
            warning_count = sum(1 for check in checks if check.status == "Warning")
            healthy_count = sum(1 for check in checks if check.status == "Healthy")
            
            if error_count > 0:
                overall_status = "Critical"
            elif warning_count > 0:
                overall_status = "Warning"
            else:
                overall_status = "Healthy"
            
            # Compile health report
            health_report = {
                'overall_status': overall_status,
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'healthy': healthy_count,
                    'warnings': warning_count,
                    'errors': error_count,
                    'total_checks': len(checks)
                },
                'components': {}
            }
            
            # Add component details
            for check in checks:
                health_report['components'][check.component] = {
                    'status': check.status,
                    'message': check.message,
                    'timestamp': check.timestamp.isoformat(),
                    'metrics': check.metrics or {}
                }
            
            logger.info(f"Health check completed: {overall_status} ({healthy_count} healthy, {warning_count} warnings, {error_count} errors)")
            
            return health_report
            
        except Exception as e:
            logger.error(f"Error during health check: {str(e)}")
            return {
                'overall_status': 'Error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'summary': {
                    'healthy': 0,
                    'warnings': 0,
                    'errors': 1,
                    'total_checks': 0
                },
                'components': {}
            }
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics"""
        try:
            metrics = {
                'timestamp': datetime.now().isoformat(),
                'system': {
                    'cpu_usage_percent': psutil.cpu_percent(interval=1),
                    'memory_usage_percent': psutil.virtual_memory().percent,
                    'disk_usage_percent': psutil.disk_usage('/').percent,
                    'boot_time': datetime.fromtimestamp(psutil.boot_time()).isoformat()
                },
                'process': {
                    'pid': os.getpid(),
                    'memory_mb': psutil.Process().memory_info().rss / (1024 * 1024),
                    'cpu_percent': psutil.Process().cpu_percent(),
                    'threads': psutil.Process().num_threads()
                }
            }
            
            # Add database metrics if available
            db_path = Path(self.settings.get_value('database', 'path', 'route_data.db'))
            if db_path.exists():
                metrics['database'] = {
                    'size_mb': db_path.stat().st_size / (1024 * 1024),
                    'modified': datetime.fromtimestamp(db_path.stat().st_mtime).isoformat()
                }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting performance metrics: {str(e)}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def start_continuous_monitoring(self, interval_minutes: int = 5) -> None:
        """Start continuous monitoring in background thread"""
        def monitor_loop():
            while self.monitoring_enabled:
                try:
                    self.health_check()
                    time.sleep(interval_minutes * 60)
                except Exception as e:
                    logger.error(f"Error in monitoring loop: {str(e)}")
                    time.sleep(60)  # Wait a minute before retrying
        
        if not hasattr(self, 'monitoring_thread') or not self.monitoring_thread.is_alive():
            self.monitoring_thread = threading.Thread(target=monitor_loop, daemon=True)
            self.monitoring_thread.start()
            logger.info(f"Started continuous monitoring (interval: {interval_minutes} minutes)")
    
    def stop_continuous_monitoring(self) -> None:
        """Stop continuous monitoring"""
        self.monitoring_enabled = False
        if hasattr(self, 'monitoring_thread'):
            self.monitoring_thread.join(timeout=10)
        logger.info("Stopped continuous monitoring")
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status"""
        return {
            'monitoring_enabled': self.monitoring_enabled,
            'monitoring_active': hasattr(self, 'monitoring_thread') and self.monitoring_thread.is_alive(),
            'last_health_check': self.health_checks[-1].timestamp.isoformat() if self.health_checks else None,
            'total_health_checks': len(self.health_checks),
            'alert_thresholds': self.alert_thresholds
        }