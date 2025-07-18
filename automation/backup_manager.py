import os
import shutil
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import json
import gzip
import tarfile

from config.settings import Settings
from utils.logger import get_logger

logger = get_logger(__name__)

class BackupManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.backup_dir = Path(self.settings.get_value('backup', 'backup_directory', 'backups'))
        self.backup_dir.mkdir(exist_ok=True)
        self.retention_days = int(self.settings.get_value('backup', 'retention_days', 30))
        self.compress_backups = self.settings.get_value('backup', 'compress_backups', 'true').lower() == 'true'
        
    def backup_database(self) -> Dict[str, Any]:
        """Create a backup of the database"""
        try:
            db_path = Path(self.settings.get_value('database', 'path', 'route_data.db'))
            
            if not db_path.exists():
                logger.error(f"Database file not found: {db_path}")
                return {
                    'success': False,
                    'error': 'Database file not found',
                    'backup_path': None
                }
            
            # Create backup filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"database_backup_{timestamp}.db"
            backup_path = self.backup_dir / backup_filename
            
            # Create database backup using SQLite backup API
            with sqlite3.connect(str(db_path)) as source_conn:
                with sqlite3.connect(str(backup_path)) as backup_conn:
                    source_conn.backup(backup_conn)
            
            # Compress backup if enabled
            if self.compress_backups:
                compressed_path = backup_path.with_suffix('.db.gz')
                with open(backup_path, 'rb') as f_in:
                    with gzip.open(compressed_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # Remove uncompressed backup
                backup_path.unlink()
                backup_path = compressed_path
            
            # Get backup size
            backup_size = backup_path.stat().st_size
            
            logger.info(f"Database backup created: {backup_path} ({backup_size} bytes)")
            
            return {
                'success': True,
                'backup_path': str(backup_path),
                'backup_size': backup_size,
                'timestamp': timestamp,
                'compressed': self.compress_backups
            }
            
        except Exception as e:
            logger.error(f"Error creating database backup: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'backup_path': None
            }
    
    def backup_reports(self) -> Dict[str, Any]:
        """Create a backup of generated reports"""
        try:
            reports_dir = Path(self.settings.get_value('reports', 'output_directory', 'data/output'))
            
            if not reports_dir.exists():
                logger.warning(f"Reports directory not found: {reports_dir}")
                return {
                    'success': True,
                    'message': 'No reports directory to backup',
                    'backup_path': None
                }
            
            # Create backup filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"reports_backup_{timestamp}.tar.gz"
            backup_path = self.backup_dir / backup_filename
            
            # Create compressed tar archive of reports
            with tarfile.open(backup_path, 'w:gz') as tar:
                tar.add(reports_dir, arcname='reports')
            
            # Get backup size
            backup_size = backup_path.stat().st_size
            
            logger.info(f"Reports backup created: {backup_path} ({backup_size} bytes)")
            
            return {
                'success': True,
                'backup_path': str(backup_path),
                'backup_size': backup_size,
                'timestamp': timestamp,
                'compressed': True
            }
            
        except Exception as e:
            logger.error(f"Error creating reports backup: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'backup_path': None
            }
    
    def backup_configuration(self) -> Dict[str, Any]:
        """Create a backup of configuration files"""
        try:
            config_dir = Path(self.settings.config_dir)
            
            # Create backup filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"config_backup_{timestamp}.tar.gz"
            backup_path = self.backup_dir / backup_filename
            
            # Create compressed tar archive of configuration
            with tarfile.open(backup_path, 'w:gz') as tar:
                tar.add(config_dir, arcname='config')
            
            # Get backup size
            backup_size = backup_path.stat().st_size
            
            logger.info(f"Configuration backup created: {backup_path} ({backup_size} bytes)")
            
            return {
                'success': True,
                'backup_path': str(backup_path),
                'backup_size': backup_size,
                'timestamp': timestamp,
                'compressed': True
            }
            
        except Exception as e:
            logger.error(f"Error creating configuration backup: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'backup_path': None
            }
    
    def backup_logs(self) -> Dict[str, Any]:
        """Create a backup of log files"""
        try:
            logs_dir = Path(self.settings.get_value('logging', 'log_directory', 'logs'))
            
            if not logs_dir.exists():
                logger.warning(f"Logs directory not found: {logs_dir}")
                return {
                    'success': True,
                    'message': 'No logs directory to backup',
                    'backup_path': None
                }
            
            # Create backup filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"logs_backup_{timestamp}.tar.gz"
            backup_path = self.backup_dir / backup_filename
            
            # Create compressed tar archive of logs
            with tarfile.open(backup_path, 'w:gz') as tar:
                tar.add(logs_dir, arcname='logs')
            
            # Get backup size
            backup_size = backup_path.stat().st_size
            
            logger.info(f"Logs backup created: {backup_path} ({backup_size} bytes)")
            
            return {
                'success': True,
                'backup_path': str(backup_path),
                'backup_size': backup_size,
                'timestamp': timestamp,
                'compressed': True
            }
            
        except Exception as e:
            logger.error(f"Error creating logs backup: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'backup_path': None
            }
    
    def full_system_backup(self) -> Dict[str, Any]:
        """Create a full system backup"""
        try:
            backup_results = {}
            
            # Backup database
            db_result = self.backup_database()
            backup_results['database'] = db_result
            
            # Backup reports
            reports_result = self.backup_reports()
            backup_results['reports'] = reports_result
            
            # Backup configuration
            config_result = self.backup_configuration()
            backup_results['configuration'] = config_result
            
            # Backup logs
            logs_result = self.backup_logs()
            backup_results['logs'] = logs_result
            
            # Calculate total backup size
            total_size = 0
            successful_backups = 0
            
            for backup_type, result in backup_results.items():
                if result.get('success', False):
                    successful_backups += 1
                    total_size += result.get('backup_size', 0)
            
            logger.info(f"Full system backup completed: {successful_backups}/4 components backed up, total size: {total_size} bytes")
            
            return {
                'success': successful_backups > 0,
                'backup_results': backup_results,
                'total_size': total_size,
                'successful_backups': successful_backups,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error creating full system backup: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'backup_results': {}
            }
    
    def cleanup_old_backups(self) -> Dict[str, Any]:
        """Remove old backup files based on retention policy"""
        try:
            if not self.backup_dir.exists():
                return {
                    'success': True,
                    'message': 'No backup directory to clean',
                    'deleted_files': []
                }
            
            cutoff_date = datetime.now() - timedelta(days=self.retention_days)
            deleted_files = []
            total_freed_space = 0
            
            # Find and delete old backup files
            for backup_file in self.backup_dir.iterdir():
                if backup_file.is_file() and backup_file.name.endswith(('.db', '.db.gz', '.tar.gz')):
                    file_mtime = datetime.fromtimestamp(backup_file.stat().st_mtime)
                    
                    if file_mtime < cutoff_date:
                        file_size = backup_file.stat().st_size
                        backup_file.unlink()
                        deleted_files.append({
                            'filename': backup_file.name,
                            'size': file_size,
                            'modified': file_mtime.isoformat()
                        })
                        total_freed_space += file_size
            
            logger.info(f"Cleaned up {len(deleted_files)} old backup files, freed {total_freed_space} bytes")
            
            return {
                'success': True,
                'deleted_files': deleted_files,
                'total_freed_space': total_freed_space,
                'retention_days': self.retention_days
            }
            
        except Exception as e:
            logger.error(f"Error cleaning up old backups: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'deleted_files': []
            }
    
    def list_backups(self) -> Dict[str, Any]:
        """List all available backups"""
        try:
            if not self.backup_dir.exists():
                return {
                    'success': True,
                    'backups': [],
                    'total_size': 0
                }
            
            backups = []
            total_size = 0
            
            for backup_file in self.backup_dir.iterdir():
                if backup_file.is_file() and backup_file.name.endswith(('.db', '.db.gz', '.tar.gz')):
                    file_stat = backup_file.stat()
                    
                    # Determine backup type
                    backup_type = 'unknown'
                    if 'database' in backup_file.name:
                        backup_type = 'database'
                    elif 'reports' in backup_file.name:
                        backup_type = 'reports'
                    elif 'config' in backup_file.name:
                        backup_type = 'configuration'
                    elif 'logs' in backup_file.name:
                        backup_type = 'logs'
                    
                    backups.append({
                        'filename': backup_file.name,
                        'type': backup_type,
                        'size': file_stat.st_size,
                        'created': datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
                        'modified': datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                        'compressed': backup_file.suffix in ['.gz', '.tar.gz']
                    })
                    
                    total_size += file_stat.st_size
            
            # Sort backups by creation date (newest first)
            backups.sort(key=lambda x: x['created'], reverse=True)
            
            return {
                'success': True,
                'backups': backups,
                'total_size': total_size,
                'backup_count': len(backups)
            }
            
        except Exception as e:
            logger.error(f"Error listing backups: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'backups': []
            }
    
    def restore_database(self, backup_filename: str) -> Dict[str, Any]:
        """Restore database from backup"""
        try:
            backup_path = self.backup_dir / backup_filename
            
            if not backup_path.exists():
                return {
                    'success': False,
                    'error': f'Backup file not found: {backup_filename}'
                }
            
            db_path = Path(self.settings.get_value('database', 'path', 'route_data.db'))
            
            # Create backup of current database
            current_backup = f"current_database_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            current_backup_path = self.backup_dir / current_backup
            
            if db_path.exists():
                shutil.copy2(db_path, current_backup_path)
                logger.info(f"Created backup of current database: {current_backup_path}")
            
            # Restore from backup
            if backup_path.suffix == '.gz':
                # Decompress and restore
                with gzip.open(backup_path, 'rb') as f_in:
                    with open(db_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                # Direct copy
                shutil.copy2(backup_path, db_path)
            
            logger.info(f"Database restored from backup: {backup_filename}")
            
            return {
                'success': True,
                'backup_filename': backup_filename,
                'restored_to': str(db_path),
                'current_backup': current_backup
            }
            
        except Exception as e:
            logger.error(f"Error restoring database: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_backup_status(self) -> Dict[str, Any]:
        """Get current backup status"""
        try:
            backup_list = self.list_backups()
            
            if not backup_list['success']:
                return backup_list
            
            # Get latest backup for each type
            latest_backups = {}
            for backup in backup_list['backups']:
                backup_type = backup['type']
                if backup_type not in latest_backups:
                    latest_backups[backup_type] = backup
            
            # Check if backups are recent (within last 24 hours)
            now = datetime.now()
            recent_threshold = now - timedelta(hours=24)
            
            status = {
                'total_backups': backup_list['backup_count'],
                'total_size': backup_list['total_size'],
                'latest_backups': latest_backups,
                'recent_backups': [],
                'old_backups': []
            }
            
            for backup in backup_list['backups']:
                backup_date = datetime.fromisoformat(backup['created'].replace('Z', '+00:00').replace('+00:00', ''))
                if backup_date > recent_threshold:
                    status['recent_backups'].append(backup)
                else:
                    status['old_backups'].append(backup)
            
            return {
                'success': True,
                'status': status,
                'retention_days': self.retention_days,
                'backup_directory': str(self.backup_dir)
            }
            
        except Exception as e:
            logger.error(f"Error getting backup status: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }