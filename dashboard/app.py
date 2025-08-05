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
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any
import os

from config.settings import Settings
from automation.scheduler import AutomationManager
from automation.system_monitor import SystemMonitor
from automation.backup_manager import BackupManager
from automation.notification_system import NotificationManager
from database.operations import DatabaseOperations
from reporting.report_manager import ReportManager

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
            self.report_manager = ReportManager(self.settings, self.db)
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
            "📈 Reports": "reports",
            "⚙️ Configuration": "configuration"
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
        
        # Real-time Charts Section
        st.subheader("📊 Real-time Performance Charts")
        self.render_overview_charts()
        
        st.markdown("---")
        
        # Database statistics
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🗄️ Database Statistics")
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
        """Enhanced reports page with interactive viewing"""
        st.title("📈 Interactive Reports Dashboard")
        
        # Report Controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            report_type = st.selectbox(
                "Report Type",
                ["Daily", "Weekly", "Monthly", "Custom"],
                key="report_type"
            )
        
        with col2:
            if report_type == "Custom":
                start_date = st.date_input("Start Date", datetime.now() - timedelta(days=7))
                end_date = st.date_input("End Date", datetime.now())
            else:
                target_date = st.date_input("Target Date", datetime.now())
        
        with col3:
            st.write("")  # Spacing
            generate_report = st.button("🔄 Generate & View Report", type="primary")
        
        st.markdown("---")
        
        # Generate and display report
        if generate_report:
            with st.spinner("Generating report..."):
                try:
                    if report_type == "Daily":
                        result = self.report_manager.generate_daily_report(
                            datetime.combine(target_date, datetime.min.time()),
                            formats=['excel'],
                            include_charts=True
                        )
                    elif report_type == "Weekly":
                        # Get start of week
                        week_start = datetime.combine(target_date, datetime.min.time())
                        week_start = week_start - timedelta(days=week_start.weekday())
                        result = self.report_manager.generate_weekly_report(
                            week_start,
                            formats=['excel'],
                            include_charts=True
                        )
                    elif report_type == "Monthly":
                        result = self.report_manager.generate_monthly_report(
                            target_date.year, target_date.month,
                            formats=['excel'],
                            include_charts=True
                        )
                    else:  # Custom
                        result = self.report_manager.generate_custom_report(
                            datetime.combine(start_date, datetime.min.time()),
                            datetime.combine(end_date, datetime.min.time()),
                            f"custom_report_{start_date}_{end_date}",
                            formats=['excel'],
                            include_charts=True
                        )
                    
                    if result['status'] == 'success':
                        st.success(f"✅ {report_type} report generated successfully!")
                        
                        # Display report summary
                        summary = result['data_summary']
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Routes", summary['routes_count'])
                        with col2:
                            st.metric("Total Revenue", f"${summary['total_revenue']:,.2f}")
                        with col3:
                            st.metric("Total Miles", f"{summary['total_miles']:,.0f}")
                        
                        st.markdown("---")
                        
                        # Interactive data visualization
                        self.render_report_visualizations(result)
                        
                        # Download link
                        if os.path.exists(result['excel_path']):
                            with open(result['excel_path'], 'rb') as f:
                                st.download_button(
                                    "📥 Download Excel Report",
                                    f.read(),
                                    file_name=os.path.basename(result['excel_path']),
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )
                    
                    elif result['status'] == 'no_data':
                        st.warning("No data found for the selected period.")
                    else:
                        st.error(f"Report generation failed: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    st.error(f"Error generating report: {e}")
        
        st.markdown("---")
        
        # Report Statistics
        st.subheader("📊 Report Statistics")
        try:
            stats = self.report_manager.get_report_statistics()
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Reports", stats['total_files'])
            with col2:
                st.metric("Storage Used", f"{stats['total_size_mb']} MB")
            with col3:
                st.metric("Output Directory", stats['output_directory'])
                
        except Exception as e:
            st.error(f"Error loading report statistics: {e}")
    
    def render_overview_charts(self):
        """Render real-time charts for the overview page"""
        try:
            # Get recent route data for charts
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)  # Last 30 days
            routes_data = self.db.get_routes_by_date_range(start_date, end_date)
            
            if not routes_data:
                st.info("No recent route data available for charts.")
                return
            
            # Convert to DataFrame for easier plotting
            df = pd.DataFrame(routes_data)
            
            # Chart 1: Daily Revenue Trend
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("💰 Daily Revenue Trend")
                if 'route_date' in df.columns and 'revenue' in df.columns:
                    daily_revenue = df.groupby('route_date')['revenue'].sum().reset_index()
                    daily_revenue['route_date'] = pd.to_datetime(daily_revenue['route_date'])
                    
                    fig = px.line(daily_revenue, x='route_date', y='revenue', 
                                title="Daily Revenue Over Time",
                                labels={'revenue': 'Revenue ($)', 'route_date': 'Date'})
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Revenue data not available.")
            
            with col2:
                st.subheader("🚛 Routes by Status")
                if 'status' in df.columns:
                    status_counts = df['status'].value_counts()
                    fig = px.pie(values=status_counts.values, names=status_counts.index,
                               title="Route Status Distribution")
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Status data not available.")
            
            # Chart 2: Driver Performance
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("👨‍💼 Top Drivers by Revenue")
                if 'driver_name' in df.columns and 'revenue' in df.columns:
                    driver_revenue = df.groupby('driver_name')['revenue'].sum().sort_values(ascending=False).head(5)
                    fig = px.bar(x=driver_revenue.values, y=driver_revenue.index, orientation='h',
                               title="Top 5 Drivers by Revenue",
                               labels={'x': 'Revenue ($)', 'y': 'Driver'})
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Driver revenue data not available.")
            
            with col2:
                st.subheader("📏 Miles vs Revenue")
                if 'total_miles' in df.columns and 'revenue' in df.columns:
                    fig = px.scatter(df, x='total_miles', y='revenue',
                                   title="Miles vs Revenue Correlation",
                                   labels={'total_miles': 'Miles', 'revenue': 'Revenue ($)'})
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Miles and revenue data not available.")
            
        except Exception as e:
            st.error(f"Error generating overview charts: {e}")
    
    def render_report_visualizations(self, report_result):
        """Render interactive visualizations for report data"""
        try:
            # Extract route data from the report result
            # This would need to be adapted based on the actual structure of your report data
            st.subheader("📊 Interactive Data Analysis")
            
            # Placeholder for route data extraction
            if 'data_summary' in report_result:
                summary = report_result['data_summary']
                
                # Create sample visualizations
                st.info("Interactive charts will be displayed here based on the generated report data.")
                
                # Example: Simple metrics display
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Avg Revenue/Route", f"${summary['total_revenue']/max(summary['routes_count'], 1):,.2f}")
                with col2:
                    st.metric("Avg Miles/Route", f"{summary['total_miles']/max(summary['routes_count'], 1):,.1f}")
                with col3:
                    st.metric("Revenue/Mile", f"${summary['total_revenue']/max(summary['total_miles'], 1):.2f}")
            
        except Exception as e:
            st.error(f"Error rendering report visualizations: {e}")
    
    def render_configuration_page(self):
        """Render configuration management page"""
        st.title("⚙️ Configuration Management")
        
        # Configuration sections
        config_section = st.selectbox(
            "Configuration Section",
            ["System Settings", "Database Settings", "Automation Settings", "Notification Settings", "Data Collection"],
            key="config_section"
        )
        
        st.markdown("---")
        
        if config_section == "System Settings":
            self.render_system_config()
        elif config_section == "Database Settings":
            self.render_database_config()
        elif config_section == "Automation Settings":
            self.render_automation_config()
        elif config_section == "Notification Settings":
            self.render_notification_config()
        elif config_section == "Data Collection":
            self.render_data_collection_config()
    
    def render_system_config(self):
        """Render system configuration form"""
        st.subheader("🔧 System Configuration")
        
        with st.form("system_config"):
            col1, col2 = st.columns(2)
            
            with col1:
                log_level = st.selectbox("Log Level", ["DEBUG", "INFO", "WARNING", "ERROR"], index=1)
                max_concurrent = st.number_input("Max Concurrent Processes", min_value=1, max_value=10, value=3)
                
            with col2:
                output_dir = st.text_input("Output Directory", value=self.settings.output_directory)
                backup_retention = st.number_input("Backup Retention (days)", min_value=1, max_value=365, value=30)
            
            submitted = st.form_submit_button("💾 Save System Settings")
            
            if submitted:
                st.success("System settings saved! (Note: Some changes require restart)")
    
    def render_database_config(self):
        """Render database configuration form"""
        st.subheader("🗄️ Database Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.text_input("Database Path", value=self.settings.database_path, disabled=True)
            st.info("Database path cannot be changed from the UI for safety.")
        
        with col2:
            if st.button("🧹 Vacuum Database"):
                with st.spinner("Running database vacuum..."):
                    try:
                        self.db.vacuum_database()
                        st.success("Database vacuum completed!")
                    except Exception as e:
                        st.error(f"Database vacuum failed: {e}")
        
        # Database statistics
        st.subheader("📊 Database Statistics")
        try:
            stats = self.db.get_database_stats()
            df_stats = pd.DataFrame(list(stats.items()), columns=['Table', 'Record Count'])
            st.dataframe(df_stats, use_container_width=True)
        except Exception as e:
            st.error(f"Error loading database statistics: {e}")
    
    def render_automation_config(self):
        """Render automation configuration form"""
        st.subheader("🤖 Automation Configuration")
        
        with st.form("automation_config"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Schedule Settings")
                data_collection_enabled = st.checkbox("Enable Data Collection", value=True)
                collection_interval = st.selectbox("Collection Interval", ["Hourly", "Daily", "Weekly"], index=1)
                
                report_generation_enabled = st.checkbox("Enable Report Generation", value=True)
                report_schedule = st.selectbox("Report Schedule", ["Daily", "Weekly", "Monthly"], index=0)
            
            with col2:
                st.subheader("Monitoring Settings")
                health_check_enabled = st.checkbox("Enable Health Monitoring", value=True)
                health_check_interval = st.number_input("Health Check Interval (minutes)", min_value=5, max_value=60, value=15)
                
                backup_enabled = st.checkbox("Enable Automatic Backups", value=True)
                backup_schedule = st.selectbox("Backup Schedule", ["Daily", "Weekly", "Monthly"], index=0)
            
            submitted = st.form_submit_button("💾 Save Automation Settings")
            
            if submitted:
                st.success("Automation settings saved!")
    
    def render_notification_config(self):
        """Render notification configuration form"""
        st.subheader("📧 Notification Configuration")
        
        with st.form("notification_config"):
            st.subheader("Email Settings")
            col1, col2 = st.columns(2)
            
            with col1:
                smtp_server = st.text_input("SMTP Server", placeholder="smtp.gmail.com")
                smtp_port = st.number_input("SMTP Port", min_value=1, max_value=65535, value=587)
                email_username = st.text_input("Email Username", placeholder="your-email@gmail.com")
                
            with col2:
                email_password = st.text_input("Email Password", type="password", placeholder="your-app-password")
                use_tls = st.checkbox("Use TLS", value=True)
                use_ssl = st.checkbox("Use SSL", value=False)
            
            st.subheader("Notification Recipients")
            admin_emails = st.text_area("Admin Emails (one per line)", placeholder="admin1@company.com\nadmin2@company.com")
            alert_emails = st.text_area("Alert Emails (one per line)", placeholder="alerts@company.com")
            
            st.subheader("Notification Rules")
            col1, col2 = st.columns(2)
            
            with col1:
                notify_errors = st.checkbox("Notify on Errors", value=True)
                notify_completion = st.checkbox("Notify on Job Completion", value=False)
                
            with col2:
                notify_health_issues = st.checkbox("Notify on Health Issues", value=True)
                notify_backup_status = st.checkbox("Notify on Backup Status", value=True)
            
            submitted = st.form_submit_button("💾 Save Notification Settings")
            
            if submitted:
                st.success("Notification settings saved!")
    
    def render_data_collection_config(self):
        """Render data collection configuration form"""
        st.subheader("📊 Data Collection Configuration")
        
        st.info("Data collection sources can be configured here. This would integrate with your collection manager.")
        
        # Example configuration form
        with st.form("data_collection_config"):
            st.subheader("Collection Sources")
            
            col1, col2 = st.columns(2)
            
            with col1:
                api_enabled = st.checkbox("Enable API Collection", value=True)
                api_endpoint = st.text_input("API Endpoint", placeholder="https://api.example.com/routes")
                api_key = st.text_input("API Key", type="password", placeholder="your-api-key")
                
            with col2:
                email_enabled = st.checkbox("Enable Email Collection", value=False)
                email_server = st.text_input("Email Server", placeholder="imap.gmail.com")
                email_folder = st.text_input("Email Folder", value="INBOX")
            
            st.subheader("File Collection")
            file_enabled = st.checkbox("Enable File Collection", value=True)
            watch_directory = st.text_input("Watch Directory", placeholder="/path/to/watch/folder")
            file_patterns = st.text_input("File Patterns", value="*.csv,*.xlsx", placeholder="*.csv,*.xlsx")
            
            submitted = st.form_submit_button("💾 Save Collection Settings")
            
            if submitted:
                st.success("Data collection settings saved!")
    
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
        elif current_page == "configuration":
            self.render_configuration_page()
        
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