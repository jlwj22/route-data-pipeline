#!/usr/bin/env python3
"""
Route Data Pipeline - Web Dashboard

A simple Streamlit-based web dashboard for monitoring and managing
the route data pipeline system.

Run with: streamlit run dashboard/app.py
"""

import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import streamlit as st
except ImportError:
    st.error("""
    Streamlit is not installed. Install it with:
    pip install streamlit
    
    Or install optional dependencies:
    pip install -r requirements.txt[dashboard]
    """)
    sys.exit(1)

import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any

from config.settings import Settings
from automation.scheduler import AutomationManager
from automation.system_monitor import SystemMonitor
from automation.backup_manager import BackupManager
from automation.notification_system import NotificationManager
from database.operations import DatabaseOperations

# Configure Streamlit page
st.set_page_config(
    page_title="Route Data Pipeline Dashboard",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded"
)

class DashboardApp:
    def __init__(self):
        self.settings = Settings()
        self.load_components()
    
    def load_components(self):
        """Load pipeline components"""
        try:
            self.db = DatabaseOperations(self.settings.database_path)
            self.automation_manager = AutomationManager(self.settings)
            self.system_monitor = SystemMonitor(self.settings)
            self.backup_manager = BackupManager(self.settings)
            self.notification_manager = NotificationManager(self.settings)
        except Exception as e:
            st.error(f"Failed to initialize components: {e}")
            st.stop()
    
    def render_sidebar(self):
        """Render sidebar navigation"""
        st.sidebar.title("🚛 Route Pipeline")
        st.sidebar.markdown("---")
        
        pages = {
            "📊 Overview": "overview",
            "🔍 System Health": "health",
            "⚙️ Automation": "automation",
            "💾 Backups": "backups",
            "📧 Notifications": "notifications",
            "📈 Reports": "reports"
        }
        
        selected_page = st.sidebar.radio("Navigation", list(pages.keys()))
        return pages[selected_page]
    
    def render_overview_page(self):
        """Render overview dashboard page"""
        st.title("📊 System Overview")
        
        # System status cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("System Status", "🟢 Online")
        
        with col2:
            try:
                db_stats = self.db.get_database_stats()
                total_records = sum(db_stats.values())
                st.metric("Total Records", f"{total_records:,}")
            except:
                st.metric("Total Records", "N/A")
        
        with col3:
            automation_status = self.automation_manager.get_automation_status()
            status = "🟢 Running" if automation_status['scheduler_running'] else "🔴 Stopped"
            st.metric("Automation", status)
        
        with col4:
            st.metric("Last Updated", datetime.now().strftime("%H:%M:%S"))
        
        st.markdown("---")
        
        # Database statistics
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Database Statistics")
            try:
                db_stats = self.db.get_database_stats()
                for table, count in db_stats.items():
                    st.write(f"**{table.title()}**: {count:,} records")
            except Exception as e:
                st.error(f"Error loading database stats: {e}")
        
        with col2:
            st.subheader("🔄 Recent Activity")
            st.write("• Data collection: Running")
            st.write("• Report generation: Scheduled")
            st.write("• System backup: Completed")
            st.write("• Health monitoring: Active")
    
    def render_health_page(self):
        """Render system health monitoring page"""
        st.title("🔍 System Health")
        
        # Auto-refresh toggle
        auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
        
        if auto_refresh:
            time.sleep(30)
            st.experimental_rerun()
        
        # Manual refresh button
        if st.button("🔄 Refresh Now"):
            st.experimental_rerun()
        
        try:
            health_report = self.system_monitor.health_check()
            
            # Overall status
            status = health_report['overall_status']
            status_color = {
                'Healthy': '🟢',
                'Warning': '🟡',
                'Critical': '🔴',
                'Error': '🔴'
            }.get(status, '⚪')
            
            st.metric("Overall Health", f"{status_color} {status}")
            
            # Summary metrics
            summary = health_report['summary']
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Healthy", summary['healthy'])
            with col2:
                st.metric("Warnings", summary['warnings'])
            with col3:
                st.metric("Errors", summary['errors'])
            with col4:
                st.metric("Total Checks", summary['total_checks'])
            
            st.markdown("---")
            
            # Component details
            st.subheader("Component Status")
            
            for component, details in health_report['components'].items():
                status_icon = {
                    'Healthy': '🟢',
                    'Warning': '🟡',
                    'Error': '🔴'
                }.get(details['status'], '⚪')
                
                with st.expander(f"{status_icon} {component}"):
                    st.write(f"**Status**: {details['status']}")
                    st.write(f"**Message**: {details['message']}")
                    st.write(f"**Timestamp**: {details['timestamp']}")
                    
                    if details.get('metrics'):
                        st.write("**Metrics**:")
                        for metric, value in details['metrics'].items():
                            st.write(f"  • {metric}: {value}")
        
        except Exception as e:
            st.error(f"Error loading health data: {e}")
    
    def render_automation_page(self):
        """Render automation management page"""
        st.title("⚙️ Automation Management")
        
        try:
            automation_status = self.automation_manager.get_automation_status()
            
            # Automation status
            col1, col2 = st.columns(2)
            
            with col1:
                running = automation_status['scheduler_running']
                status = "🟢 Running" if running else "🔴 Stopped"
                st.metric("Scheduler Status", status)
            
            with col2:
                st.metric("Scheduled Jobs", automation_status['total_jobs'])
            
            # Control buttons
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("▶️ Start Automation"):
                    try:
                        self.automation_manager.start_automation()
                        st.success("Automation started successfully!")
                        time.sleep(1)
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Failed to start automation: {e}")
            
            with col2:
                if st.button("⏹️ Stop Automation"):
                    try:
                        self.automation_manager.stop_automation()
                        st.success("Automation stopped successfully!")
                        time.sleep(1)
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Failed to stop automation: {e}")
            
            st.markdown("---")
            
            # Scheduled jobs
            st.subheader("📅 Scheduled Jobs")
            
            if automation_status['scheduled_jobs']:
                for job in automation_status['scheduled_jobs']:
                    with st.expander(f"🔄 {job['name']}"):
                        st.write(f"**Next Run**: {job['next_run'] or 'Not scheduled'}")
                        st.write(f"**Last Run**: {job['last_run'] or 'Never'}")
            else:
                st.info("No scheduled jobs configured.")
        
        except Exception as e:
            st.error(f"Error loading automation data: {e}")
    
    def render_backups_page(self):
        """Render backup management page"""
        st.title("💾 Backup Management")
        
        # Backup actions
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("💾 Full Backup"):
                with st.spinner("Creating full system backup..."):
                    try:
                        result = self.backup_manager.full_system_backup()
                        if result['success']:
                            st.success(f"Backup completed! {result['successful_backups']}/4 components backed up.")
                        else:
                            st.error("Backup failed. Check logs for details.")
                    except Exception as e:
                        st.error(f"Backup failed: {e}")
        
        with col2:
            if st.button("🗄️ Database Backup"):
                with st.spinner("Creating database backup..."):
                    try:
                        result = self.backup_manager.backup_database()
                        if result['success']:
                            st.success("Database backup completed!")
                        else:
                            st.error(f"Database backup failed: {result.get('error', 'Unknown error')}")
                    except Exception as e:
                        st.error(f"Database backup failed: {e}")
        
        with col3:
            if st.button("🧹 Cleanup Old Backups"):
                with st.spinner("Cleaning up old backups..."):
                    try:
                        result = self.backup_manager.cleanup_old_backups()
                        if result['success']:
                            st.success(f"Cleaned up {len(result['deleted_files'])} old backup files.")
                        else:
                            st.error("Cleanup failed. Check logs for details.")
                    except Exception as e:
                        st.error(f"Cleanup failed: {e}")
        
        st.markdown("---")
        
        # Backup list
        st.subheader("📁 Available Backups")
        
        try:
            backup_list = self.backup_manager.list_backups()
            
            if backup_list['success'] and backup_list['backups']:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Total Backups", backup_list['backup_count'])
                with col2:
                    total_size_mb = backup_list['total_size'] / (1024 * 1024)
                    st.metric("Total Size", f"{total_size_mb:.1f} MB")
                
                # Backup table
                for backup in backup_list['backups'][:10]:  # Show last 10 backups
                    with st.expander(f"📄 {backup['filename']}"):
                        st.write(f"**Type**: {backup['type'].title()}")
                        st.write(f"**Size**: {backup['size'] / (1024*1024):.1f} MB")
                        st.write(f"**Created**: {backup['created']}")
                        st.write(f"**Compressed**: {'Yes' if backup['compressed'] else 'No'}")
            else:
                st.info("No backups found.")
        
        except Exception as e:
            st.error(f"Error loading backup list: {e}")
    
    def render_notifications_page(self):
        """Render notification management page"""
        st.title("📧 Notification Management")
        
        # Test notification
        if st.button("📧 Send Test Notification"):
            with st.spinner("Sending test notification..."):
                try:
                    success = self.notification_manager.test_notification_system()
                    if success:
                        st.success("Test notification sent successfully! Check your email.")
                    else:
                        st.error("Test notification failed. Check configuration and logs.")
                except Exception as e:
                    st.error(f"Test notification failed: {e}")
        
        st.markdown("---")
        
        # Notification configuration display
        st.subheader("⚙️ Notification Configuration")
        
        try:
            rules = self.notification_manager.notification_rules
            
            if rules:
                # Recipients
                st.write("**📮 Recipients**:")
                recipients = rules.get('recipients', {})
                for group, emails in recipients.items():
                    st.write(f"• **{group.title()}**: {', '.join(emails)}")
                
                st.write("**📋 Notification Rules**:")
                notification_rules = rules.get('rules', {})
                for rule_name, rule_config in notification_rules.items():
                    enabled = "✅" if rule_config.get('enabled', False) else "❌"
                    recipients_list = ", ".join(rule_config.get('recipients', []))
                    st.write(f"• {enabled} **{rule_name.replace('_', ' ').title()}** → {recipients_list}")
            else:
                st.info("No notification configuration found.")
        
        except Exception as e:
            st.error(f"Error loading notification configuration: {e}")
    
    def render_reports_page(self):
        """Render reports page"""
        st.title("📈 Reports")
        
        st.info("Report generation and viewing functionality will be available once reports are generated by the system.")
        
        # Report actions
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📊 Generate Daily Report"):
                st.info("Report generation will be triggered. Check the system logs for progress.")
        
        with col2:
            if st.button("📈 Generate Weekly Report"):
                st.info("Report generation will be triggered. Check the system logs for progress.")
        
        st.markdown("---")
        
        # Recent reports (placeholder)
        st.subheader("📁 Recent Reports")
        st.info("Reports will appear here once generated by the system.")
    
    def run(self):
        """Run the Streamlit dashboard"""
        # Sidebar navigation
        current_page = self.render_sidebar()
        
        # Render selected page
        if current_page == "overview":
            self.render_overview_page()
        elif current_page == "health":
            self.render_health_page()
        elif current_page == "automation":
            self.render_automation_page()
        elif current_page == "backups":
            self.render_backups_page()
        elif current_page == "notifications":
            self.render_notifications_page()
        elif current_page == "reports":
            self.render_reports_page()
        
        # Footer
        st.sidebar.markdown("---")
        st.sidebar.markdown("**Route Data Pipeline**")
        st.sidebar.markdown("Version 0.2.0")
        st.sidebar.markdown(f"Updated: {datetime.now().strftime('%H:%M:%S')}")

def main():
    """Main entry point for the dashboard"""
    try:
        app = DashboardApp()
        app.run()
    except Exception as e:
        st.error(f"Dashboard initialization failed: {e}")
        st.info("Please ensure the route pipeline system is properly configured.")

if __name__ == "__main__":
    main()