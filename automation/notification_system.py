import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
import json
from pathlib import Path

from config.settings import Settings
from utils.logger import get_logger

logger = get_logger(__name__)

class EmailNotificationSystem:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.smtp_server = None
        self.smtp_port = None
        self.sender_email = None
        self.sender_password = None
        self.use_tls = True
        self.load_email_config()
    
    def load_email_config(self) -> None:
        """Load email configuration from settings"""
        try:
            # Get email configuration from settings
            email_config = self.settings.get_section('email')
            
            self.smtp_server = email_config.get('smtp_server', 'smtp.gmail.com')
            self.smtp_port = int(email_config.get('smtp_port', 587))
            self.sender_email = email_config.get('sender_email')
            self.sender_password = email_config.get('sender_password')
            self.use_tls = email_config.get('use_tls', 'true').lower() == 'true'
            
            if not self.sender_email or not self.sender_password:
                logger.warning("Email credentials not configured. Notifications will not be sent.")
                
        except Exception as e:
            logger.error(f"Error loading email configuration: {str(e)}")
    
    def send_email(self, recipients: List[str], subject: str, body: str, 
                   html_body: Optional[str] = None, attachments: Optional[List[str]] = None) -> bool:
        """Send email notification"""
        if not self.sender_email or not self.sender_password:
            logger.warning("Email not configured. Cannot send notification.")
            return False
        
        try:
            # Create message
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = self.sender_email
            message["To"] = ", ".join(recipients)
            
            # Add text body
            text_part = MIMEText(body, "plain")
            message.attach(text_part)
            
            # Add HTML body if provided
            if html_body:
                html_part = MIMEText(html_body, "html")
                message.attach(html_part)
            
            # Add attachments if provided
            if attachments:
                for attachment_path in attachments:
                    if Path(attachment_path).exists():
                        with open(attachment_path, "rb") as attachment:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(attachment.read())
                        
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {Path(attachment_path).name}'
                        )
                        message.attach(part)
            
            # Create secure connection and send email
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls(context=context)
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, recipients, message.as_string())
            
            logger.info(f"Email sent successfully to {recipients}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            return False
    
    def send_report_notification(self, report_info: Dict[str, Any], recipients: List[str]) -> bool:
        """Send notification about generated report"""
        subject = f"Route Data Pipeline Report - {report_info.get('type', 'Unknown')} - {datetime.now().strftime('%Y-%m-%d')}"
        
        body = f"""
Route Data Pipeline Report Generated

Report Details:
- Type: {report_info.get('type', 'Unknown')}
- Generated: {report_info.get('generated_at', 'Unknown')}
- Records Processed: {report_info.get('record_count', 'Unknown')}
- File Location: {report_info.get('file_path', 'Unknown')}

Summary:
{report_info.get('summary', 'No summary available')}

This is an automated message from the Route Data Pipeline system.
        """
        
        html_body = f"""
        <html>
        <head></head>
        <body>
            <h2>Route Data Pipeline Report Generated</h2>
            
            <h3>Report Details:</h3>
            <ul>
                <li><strong>Type:</strong> {report_info.get('type', 'Unknown')}</li>
                <li><strong>Generated:</strong> {report_info.get('generated_at', 'Unknown')}</li>
                <li><strong>Records Processed:</strong> {report_info.get('record_count', 'Unknown')}</li>
                <li><strong>File Location:</strong> {report_info.get('file_path', 'Unknown')}</li>
            </ul>
            
            <h3>Summary:</h3>
            <p>{report_info.get('summary', 'No summary available')}</p>
            
            <hr>
            <p><em>This is an automated message from the Route Data Pipeline system.</em></p>
        </body>
        </html>
        """
        
        attachments = [report_info.get('file_path')] if report_info.get('file_path') else None
        
        return self.send_email(recipients, subject, body, html_body, attachments)
    
    def send_error_notification(self, error_info: Dict[str, Any], recipients: List[str]) -> bool:
        """Send notification about system errors"""
        subject = f"Route Data Pipeline Error Alert - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        body = f"""
Route Data Pipeline Error Alert

Error Details:
- Component: {error_info.get('component', 'Unknown')}
- Error Type: {error_info.get('error_type', 'Unknown')}
- Occurred: {error_info.get('timestamp', 'Unknown')}
- Message: {error_info.get('message', 'No message available')}

Stack Trace:
{error_info.get('stack_trace', 'No stack trace available')}

Please investigate this issue as soon as possible.

This is an automated message from the Route Data Pipeline system.
        """
        
        html_body = f"""
        <html>
        <head></head>
        <body>
            <h2 style="color: red;">Route Data Pipeline Error Alert</h2>
            
            <h3>Error Details:</h3>
            <ul>
                <li><strong>Component:</strong> {error_info.get('component', 'Unknown')}</li>
                <li><strong>Error Type:</strong> {error_info.get('error_type', 'Unknown')}</li>
                <li><strong>Occurred:</strong> {error_info.get('timestamp', 'Unknown')}</li>
                <li><strong>Message:</strong> {error_info.get('message', 'No message available')}</li>
            </ul>
            
            <h3>Stack Trace:</h3>
            <pre style="background-color: #f0f0f0; padding: 10px; border-radius: 5px;">
{error_info.get('stack_trace', 'No stack trace available')}
            </pre>
            
            <p style="color: red;"><strong>Please investigate this issue as soon as possible.</strong></p>
            
            <hr>
            <p><em>This is an automated message from the Route Data Pipeline system.</em></p>
        </body>
        </html>
        """
        
        return self.send_email(recipients, subject, body, html_body)
    
    def send_system_status_notification(self, status_info: Dict[str, Any], recipients: List[str]) -> bool:
        """Send system status notification"""
        subject = f"Route Data Pipeline System Status - {datetime.now().strftime('%Y-%m-%d')}"
        
        body = f"""
Route Data Pipeline System Status Report

System Health: {status_info.get('health_status', 'Unknown')}
Last Updated: {status_info.get('last_updated', 'Unknown')}

Components Status:
{self._format_component_status(status_info.get('components', {}))}

Performance Metrics:
{self._format_performance_metrics(status_info.get('metrics', {}))}

This is an automated message from the Route Data Pipeline system.
        """
        
        html_body = f"""
        <html>
        <head></head>
        <body>
            <h2>Route Data Pipeline System Status Report</h2>
            
            <h3>System Health: <span style="color: {'green' if status_info.get('health_status') == 'Healthy' else 'red'};">{status_info.get('health_status', 'Unknown')}</span></h3>
            <p><strong>Last Updated:</strong> {status_info.get('last_updated', 'Unknown')}</p>
            
            <h3>Components Status:</h3>
            {self._format_component_status_html(status_info.get('components', {}))}
            
            <h3>Performance Metrics:</h3>
            {self._format_performance_metrics_html(status_info.get('metrics', {}))}
            
            <hr>
            <p><em>This is an automated message from the Route Data Pipeline system.</em></p>
        </body>
        </html>
        """
        
        return self.send_email(recipients, subject, body, html_body)
    
    def _format_component_status(self, components: Dict[str, Any]) -> str:
        """Format component status for text email"""
        status_text = ""
        for component, status in components.items():
            status_text += f"- {component}: {status}\n"
        return status_text
    
    def _format_component_status_html(self, components: Dict[str, Any]) -> str:
        """Format component status for HTML email"""
        status_html = "<ul>"
        for component, status in components.items():
            color = "green" if status == "Healthy" else "red"
            status_html += f"<li><strong>{component}:</strong> <span style=\"color: {color};\">{status}</span></li>"
        status_html += "</ul>"
        return status_html
    
    def _format_performance_metrics(self, metrics: Dict[str, Any]) -> str:
        """Format performance metrics for text email"""
        metrics_text = ""
        for metric, value in metrics.items():
            metrics_text += f"- {metric}: {value}\n"
        return metrics_text
    
    def _format_performance_metrics_html(self, metrics: Dict[str, Any]) -> str:
        """Format performance metrics for HTML email"""
        metrics_html = "<ul>"
        for metric, value in metrics.items():
            metrics_html += f"<li><strong>{metric}:</strong> {value}</li>"
        metrics_html += "</ul>"
        return metrics_html

class NotificationManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.email_system = EmailNotificationSystem(settings)
        self.notification_rules = self.load_notification_rules()
    
    def load_notification_rules(self) -> Dict[str, Any]:
        """Load notification rules from configuration"""
        config_file = Path(self.settings.config_dir) / 'notification_rules.json'
        
        if not config_file.exists():
            # Create default notification rules
            default_rules = {
                'recipients': {
                    'admins': ['admin@example.com'],
                    'operators': ['operator@example.com'],
                    'reports': ['reports@example.com']
                },
                'rules': {
                    'report_generated': {
                        'enabled': True,
                        'recipients': ['reports', 'admins'],
                        'conditions': []
                    },
                    'error_occurred': {
                        'enabled': True,
                        'recipients': ['admins'],
                        'conditions': ['severity >= WARNING']
                    },
                    'system_health_check': {
                        'enabled': True,
                        'recipients': ['admins'],
                        'conditions': ['health_status != Healthy']
                    },
                    'data_collection_failed': {
                        'enabled': True,
                        'recipients': ['admins', 'operators'],
                        'conditions': []
                    }
                }
            }
            
            with open(config_file, 'w') as f:
                json.dump(default_rules, f, indent=2)
            
            logger.info(f"Created default notification rules: {config_file}")
        
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading notification rules: {str(e)}")
            return {}
    
    def _get_recipients_for_rule(self, rule_name: str) -> List[str]:
        """Get email recipients for a notification rule"""
        rule = self.notification_rules.get('rules', {}).get(rule_name, {})
        recipient_groups = rule.get('recipients', [])
        
        recipients = []
        for group in recipient_groups:
            group_emails = self.notification_rules.get('recipients', {}).get(group, [])
            recipients.extend(group_emails)
        
        return list(set(recipients))  # Remove duplicates
    
    def send_report_notification(self, report_info: Dict[str, Any]) -> bool:
        """Send notification about generated report"""
        if not self.notification_rules.get('rules', {}).get('report_generated', {}).get('enabled', False):
            return True
        
        recipients = self._get_recipients_for_rule('report_generated')
        if not recipients:
            logger.warning("No recipients configured for report notifications")
            return False
        
        return self.email_system.send_report_notification(report_info, recipients)
    
    def send_error_notification(self, error_info: Dict[str, Any]) -> bool:
        """Send notification about system errors"""
        if not self.notification_rules.get('rules', {}).get('error_occurred', {}).get('enabled', False):
            return True
        
        recipients = self._get_recipients_for_rule('error_occurred')
        if not recipients:
            logger.warning("No recipients configured for error notifications")
            return False
        
        return self.email_system.send_error_notification(error_info, recipients)
    
    def send_system_status_notification(self, status_info: Dict[str, Any]) -> bool:
        """Send system status notification"""
        if not self.notification_rules.get('rules', {}).get('system_health_check', {}).get('enabled', False):
            return True
        
        # Only send if health status is not healthy
        if status_info.get('health_status') == 'Healthy':
            return True
        
        recipients = self._get_recipients_for_rule('system_health_check')
        if not recipients:
            logger.warning("No recipients configured for system status notifications")
            return False
        
        return self.email_system.send_system_status_notification(status_info, recipients)
    
    def send_data_collection_failed_notification(self, error_info: Dict[str, Any]) -> bool:
        """Send notification about data collection failures"""
        if not self.notification_rules.get('rules', {}).get('data_collection_failed', {}).get('enabled', False):
            return True
        
        recipients = self._get_recipients_for_rule('data_collection_failed')
        if not recipients:
            logger.warning("No recipients configured for data collection failure notifications")
            return False
        
        return self.email_system.send_error_notification(error_info, recipients)
    
    def test_notification_system(self) -> bool:
        """Test the notification system"""
        test_recipients = self.notification_rules.get('recipients', {}).get('admins', [])
        
        if not test_recipients:
            logger.error("No admin recipients configured for testing")
            return False
        
        subject = "Route Data Pipeline - Notification System Test"
        body = f"""
This is a test email from the Route Data Pipeline notification system.

Test Details:
- Timestamp: {datetime.now().isoformat()}
- System: Route Data Pipeline
- Component: Notification System

If you received this email, the notification system is working correctly.
        """
        
        html_body = f"""
        <html>
        <head></head>
        <body>
            <h2>Route Data Pipeline - Notification System Test</h2>
            
            <p>This is a test email from the Route Data Pipeline notification system.</p>
            
            <h3>Test Details:</h3>
            <ul>
                <li><strong>Timestamp:</strong> {datetime.now().isoformat()}</li>
                <li><strong>System:</strong> Route Data Pipeline</li>
                <li><strong>Component:</strong> Notification System</li>
            </ul>
            
            <p style="color: green;"><strong>If you received this email, the notification system is working correctly.</strong></p>
        </body>
        </html>
        """
        
        return self.email_system.send_email(test_recipients, subject, body, html_body)