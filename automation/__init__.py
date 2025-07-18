"""
Automation module for the Route Data Pipeline System.

This module provides comprehensive automation capabilities including:
- Task scheduling with configurable intervals
- Email notification system for reports and alerts
- Automated backup processes for database and reports
- System monitoring and health checks

Components:
- scheduler: Task scheduling and automation management
- notification_system: Email notifications for reports and alerts
- backup_manager: Automated backup processes
- system_monitor: System health monitoring and checks
"""

from .scheduler import TaskScheduler, AutomationManager
from .notification_system import EmailNotificationSystem, NotificationManager
from .backup_manager import BackupManager
from .system_monitor import SystemMonitor

__all__ = [
    'TaskScheduler',
    'AutomationManager',
    'EmailNotificationSystem',
    'NotificationManager',
    'BackupManager',
    'SystemMonitor'
]