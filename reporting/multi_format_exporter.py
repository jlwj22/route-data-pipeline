"""
Multi-format exporter for route pipeline reports.
Supports Excel, CSV, PDF, and HTML export formats.
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from io import BytesIO
import json
import csv

# PDF support
try:
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

# HTML template support
try:
    from jinja2 import Template, Environment, FileSystemLoader
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger

logger = get_logger(__name__)

class MultiFormatExporter:
    """
    Export reports in multiple formats: Excel, CSV, PDF, HTML, JSON.
    """
    
    def __init__(self, output_directory: str = None):
        self.output_directory = output_directory or 'data/output'
        
        # Ensure output directory exists
        os.makedirs(self.output_directory, exist_ok=True)
        
        # HTML template directory
        self.template_directory = os.path.join(os.path.dirname(__file__), 'html_templates')
        os.makedirs(self.template_directory, exist_ok=True)
        
        # PDF styles
        if PDF_AVAILABLE:
            self.pdf_styles = getSampleStyleSheet()
            self._setup_pdf_styles()
        
        # HTML templates
        if JINJA2_AVAILABLE:
            self.jinja_env = Environment(loader=FileSystemLoader(self.template_directory))
        
        logger.info(f"Multi-format exporter initialized with output directory: {self.output_directory}")
    
    def export_report(self, data: Dict[str, Any], report_name: str, 
                     formats: List[str] = None, metadata: Dict[str, Any] = None) -> Dict[str, str]:
        """
        Export report data in multiple formats.
        
        Args:
            data: Report data dictionary
            report_name: Base name for the report files
            formats: List of formats to export ['excel', 'csv', 'pdf', 'html', 'json']
            metadata: Additional metadata for the report
            
        Returns:
            Dictionary mapping format names to file paths
        """
        if formats is None:
            formats = ['excel', 'csv', 'html', 'json']
        
        logger.info(f"Exporting report '{report_name}' in formats: {formats}")
        
        exported_files = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for format_name in formats:
            try:
                file_path = self._export_single_format(data, report_name, format_name, timestamp, metadata)
                if file_path:
                    exported_files[format_name] = file_path
                    logger.info(f"Exported {format_name}: {file_path}")
            except Exception as e:
                logger.error(f"Error exporting {format_name} format: {e}")
        
        return exported_files
    
    def _export_single_format(self, data: Dict[str, Any], report_name: str, 
                             format_name: str, timestamp: str, metadata: Dict[str, Any] = None) -> Optional[str]:
        """Export data in a single format."""
        
        if format_name.lower() == 'excel':
            return self._export_excel(data, report_name, timestamp, metadata)
        elif format_name.lower() == 'csv':
            return self._export_csv(data, report_name, timestamp, metadata)
        elif format_name.lower() == 'pdf':
            return self._export_pdf(data, report_name, timestamp, metadata)
        elif format_name.lower() == 'html':
            return self._export_html(data, report_name, timestamp, metadata)
        elif format_name.lower() == 'json':
            return self._export_json(data, report_name, timestamp, metadata)
        else:
            logger.warning(f"Unsupported format: {format_name}")
            return None
    
    def _export_excel(self, data: Dict[str, Any], report_name: str, 
                     timestamp: str, metadata: Dict[str, Any] = None) -> str:
        """Export data as Excel file."""
        file_path = os.path.join(self.output_directory, f"{report_name}_{timestamp}.xlsx")
        
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            # Write main data
            main_data = data.get('routes', [])
            if main_data:
                df = pd.DataFrame(main_data)
                df.to_excel(writer, sheet_name='Routes', index=False)
            
            # Write summary data
            summary_data = data.get('summary', {})
            if summary_data:
                summary_df = pd.DataFrame([summary_data])
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Write driver data
            driver_data = data.get('drivers', [])
            if driver_data:
                driver_df = pd.DataFrame(driver_data)
                driver_df.to_excel(writer, sheet_name='Drivers', index=False)
            
            # Write vehicle data
            vehicle_data = data.get('vehicles', [])
            if vehicle_data:
                vehicle_df = pd.DataFrame(vehicle_data)
                vehicle_df.to_excel(writer, sheet_name='Vehicles', index=False)
            
            # Write customer data
            customer_data = data.get('customers', [])
            if customer_data:
                customer_df = pd.DataFrame(customer_data)
                customer_df.to_excel(writer, sheet_name='Customers', index=False)
            
            # Write metadata
            if metadata:
                metadata_df = pd.DataFrame([metadata])
                metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
        
        return file_path
    
    def _export_csv(self, data: Dict[str, Any], report_name: str, 
                   timestamp: str, metadata: Dict[str, Any] = None) -> str:
        """Export data as CSV files (one per data type)."""
        output_dir = os.path.join(self.output_directory, f"{report_name}_{timestamp}_csv")
        os.makedirs(output_dir, exist_ok=True)
        
        exported_files = []
        
        # Export routes data
        routes_data = data.get('routes', [])
        if routes_data:
            routes_file = os.path.join(output_dir, 'routes.csv')
            df = pd.DataFrame(routes_data)
            df.to_csv(routes_file, index=False)
            exported_files.append(routes_file)
        
        # Export summary data
        summary_data = data.get('summary', {})
        if summary_data:
            summary_file = os.path.join(output_dir, 'summary.csv')
            df = pd.DataFrame([summary_data])
            df.to_csv(summary_file, index=False)
            exported_files.append(summary_file)
        
        # Export driver data
        driver_data = data.get('drivers', [])
        if driver_data:
            drivers_file = os.path.join(output_dir, 'drivers.csv')
            df = pd.DataFrame(driver_data)
            df.to_csv(drivers_file, index=False)
            exported_files.append(drivers_file)
        
        # Export vehicle data
        vehicle_data = data.get('vehicles', [])
        if vehicle_data:
            vehicles_file = os.path.join(output_dir, 'vehicles.csv')
            df = pd.DataFrame(vehicle_data)
            df.to_csv(vehicles_file, index=False)
            exported_files.append(vehicles_file)
        
        # Export customer data
        customer_data = data.get('customers', [])
        if customer_data:
            customers_file = os.path.join(output_dir, 'customers.csv')
            df = pd.DataFrame(customer_data)
            df.to_csv(customers_file, index=False)
            exported_files.append(customers_file)
        
        # Create a manifest file
        manifest_file = os.path.join(output_dir, 'manifest.txt')
        with open(manifest_file, 'w') as f:
            f.write(f"Report: {report_name}\n")
            f.write(f"Generated: {timestamp}\n")
            f.write(f"Files:\n")
            for file_path in exported_files:
                f.write(f"  - {os.path.basename(file_path)}\n")
        
        return output_dir
    
    def _export_pdf(self, data: Dict[str, Any], report_name: str, 
                   timestamp: str, metadata: Dict[str, Any] = None) -> Optional[str]:
        """Export data as PDF file."""
        if not PDF_AVAILABLE:
            logger.warning("PDF export not available. Install reportlab: pip install reportlab")
            return None
        
        file_path = os.path.join(self.output_directory, f"{report_name}_{timestamp}.pdf")
        
        doc = SimpleDocTemplate(file_path, pagesize=letter)
        story = []
        
        # Title
        title_style = self.pdf_styles['Title']
        title = Paragraph(f"{report_name} Report", title_style)
        story.append(title)
        story.append(Spacer(1, 12))
        
        # Metadata
        if metadata:
            meta_style = self.pdf_styles['Normal']
            story.append(Paragraph(f"Generated: {metadata.get('generated_at', timestamp)}", meta_style))
            story.append(Paragraph(f"Report Period: {metadata.get('report_period', 'N/A')}", meta_style))
            story.append(Spacer(1, 12))
        
        # Summary section
        summary_data = data.get('summary', {})
        if summary_data:
            story.append(Paragraph("Summary", self.pdf_styles['Heading2']))
            
            summary_table_data = []
            for key, value in summary_data.items():
                if isinstance(value, (int, float)):
                    if key.lower() in ['revenue', 'cost', 'profit']:
                        value = f"${value:,.2f}"
                    elif key.lower() in ['miles', 'distance']:
                        value = f"{value:,.2f}"
                    else:
                        value = f"{value:,.0f}"
                summary_table_data.append([key.replace('_', ' ').title(), str(value)])
            
            if summary_table_data:
                summary_table = Table(summary_table_data, colWidths=[3*inch, 2*inch])
                summary_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 12),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                story.append(summary_table)
                story.append(Spacer(1, 12))
        
        # Routes section
        routes_data = data.get('routes', [])
        if routes_data:
            story.append(Paragraph("Routes", self.pdf_styles['Heading2']))
            
            # Create routes table
            routes_table_data = []
            if routes_data:
                # Headers
                headers = ['Route ID', 'Driver', 'Miles', 'Revenue', 'Status']
                routes_table_data.append(headers)
                
                # Data rows (limit to first 20 for PDF)
                for route in routes_data[:20]:
                    row = [
                        str(route.get('id', '')),
                        str(route.get('driver_name', '')),
                        f"{route.get('total_miles', 0):.2f}",
                        f"${route.get('revenue', 0):.2f}",
                        str(route.get('status', ''))
                    ]
                    routes_table_data.append(row)
                
                routes_table = Table(routes_table_data, colWidths=[1*inch, 1.5*inch, 1*inch, 1*inch, 1*inch])
                routes_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 8)
                ]))
                story.append(routes_table)
                
                if len(routes_data) > 20:
                    story.append(Spacer(1, 6))
                    story.append(Paragraph(f"... and {len(routes_data) - 20} more routes", self.pdf_styles['Normal']))
        
        # Driver performance section
        driver_data = data.get('drivers', [])
        if driver_data:
            story.append(Spacer(1, 12))
            story.append(Paragraph("Driver Performance", self.pdf_styles['Heading2']))
            
            driver_table_data = []
            headers = ['Driver', 'Routes', 'Miles', 'Revenue', 'Efficiency']
            driver_table_data.append(headers)
            
            for driver in driver_data[:10]:  # Limit to top 10
                row = [
                    str(driver.get('name', '')),
                    str(driver.get('route_count', 0)),
                    f"{driver.get('total_miles', 0):.2f}",
                    f"${driver.get('revenue', 0):.2f}",
                    f"{driver.get('efficiency', 0):.1f}%"
                ]
                driver_table_data.append(row)
            
            driver_table = Table(driver_table_data, colWidths=[1.5*inch, 0.8*inch, 1*inch, 1*inch, 1*inch])
            driver_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 8)
            ]))
            story.append(driver_table)
        
        # Build PDF
        doc.build(story)
        
        return file_path
    
    def _export_html(self, data: Dict[str, Any], report_name: str, 
                    timestamp: str, metadata: Dict[str, Any] = None) -> str:
        """Export data as HTML file."""
        file_path = os.path.join(self.output_directory, f"{report_name}_{timestamp}.html")
        
        # Create HTML template if it doesn't exist
        template_path = os.path.join(self.template_directory, 'report_template.html')
        if not os.path.exists(template_path):
            self._create_default_html_template(template_path)
        
        # Generate HTML content
        html_content = self._generate_html_content(data, report_name, timestamp, metadata)
        
        with open(file_path, 'w') as f:
            f.write(html_content)
        
        return file_path
    
    def _export_json(self, data: Dict[str, Any], report_name: str, 
                    timestamp: str, metadata: Dict[str, Any] = None) -> str:
        """Export data as JSON file."""
        file_path = os.path.join(self.output_directory, f"{report_name}_{timestamp}.json")
        
        # Prepare data for JSON export
        json_data = {
            'report_name': report_name,
            'generated_at': timestamp,
            'metadata': metadata or {},
            'data': data
        }
        
        with open(file_path, 'w') as f:
            json.dump(json_data, f, indent=2, default=str)
        
        return file_path
    
    def _setup_pdf_styles(self):
        """Setup custom PDF styles."""
        self.pdf_styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.pdf_styles['Title'],
            fontSize=18,
            textColor=colors.darkblue,
            alignment=TA_CENTER
        ))
        
        self.pdf_styles.add(ParagraphStyle(
            name='CustomHeading',
            parent=self.pdf_styles['Heading1'],
            fontSize=14,
            textColor=colors.darkblue,
            spaceBefore=12,
            spaceAfter=6
        ))
    
    def _create_default_html_template(self, template_path: str):
        """Create a default HTML template."""
        template_content = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ report_name }} Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background-color: #f4f4f4; padding: 20px; text-align: center; }
        .summary { background-color: #e8f4f8; padding: 15px; margin: 20px 0; }
        .section { margin: 20px 0; }
        .table { width: 100%; border-collapse: collapse; }
        .table th, .table td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        .table th { background-color: #4CAF50; color: white; }
        .table tr:nth-child(even) { background-color: #f2f2f2; }
        .metric { display: inline-block; margin: 10px; padding: 10px; background-color: #f9f9f9; border-radius: 5px; }
        .metric-value { font-size: 24px; font-weight: bold; color: #2E86AB; }
        .metric-label { font-size: 14px; color: #666; }
    </style>
</head>
<body>
    <div class="header">
        <h1>{{ report_name }} Report</h1>
        <p>Generated: {{ generated_at }}</p>
    </div>
    
    {% if summary %}
    <div class="summary">
        <h2>Summary</h2>
        <div class="metrics">
            {% for key, value in summary.items() %}
            <div class="metric">
                <div class="metric-value">{{ value }}</div>
                <div class="metric-label">{{ key.replace('_', ' ').title() }}</div>
            </div>
            {% endfor %}
        </div>
    </div>
    {% endif %}
    
    {% if routes %}
    <div class="section">
        <h2>Routes</h2>
        <table class="table">
            <thead>
                <tr>
                    <th>Route ID</th>
                    <th>Driver</th>
                    <th>Miles</th>
                    <th>Revenue</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {% for route in routes %}
                <tr>
                    <td>{{ route.id }}</td>
                    <td>{{ route.driver_name }}</td>
                    <td>{{ route.total_miles }}</td>
                    <td>${{ route.revenue }}</td>
                    <td>{{ route.status }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
    {% endif %}
    
    {% if drivers %}
    <div class="section">
        <h2>Driver Performance</h2>
        <table class="table">
            <thead>
                <tr>
                    <th>Driver</th>
                    <th>Routes</th>
                    <th>Miles</th>
                    <th>Revenue</th>
                    <th>Efficiency</th>
                </tr>
            </thead>
            <tbody>
                {% for driver in drivers %}
                <tr>
                    <td>{{ driver.name }}</td>
                    <td>{{ driver.route_count }}</td>
                    <td>{{ driver.total_miles }}</td>
                    <td>${{ driver.revenue }}</td>
                    <td>{{ driver.efficiency }}%</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
    {% endif %}
</body>
</html>'''
        
        with open(template_path, 'w') as f:
            f.write(template_content)
    
    def _generate_html_content(self, data: Dict[str, Any], report_name: str, 
                              timestamp: str, metadata: Dict[str, Any] = None) -> str:
        """Generate HTML content from data."""
        
        if JINJA2_AVAILABLE:
            try:
                template = self.jinja_env.get_template('report_template.html')
                return template.render(
                    report_name=report_name,
                    generated_at=timestamp,
                    metadata=metadata,
                    summary=data.get('summary', {}),
                    routes=data.get('routes', []),
                    drivers=data.get('drivers', []),
                    vehicles=data.get('vehicles', []),
                    customers=data.get('customers', [])
                )
            except Exception as e:
                logger.error(f"Error using Jinja2 template: {e}")
        
        # Fallback to simple HTML generation
        html_content = f'''<!DOCTYPE html>
<html>
<head>
    <title>{report_name} Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f4f4f4; padding: 20px; text-align: center; }}
        .summary {{ background-color: #e8f4f8; padding: 15px; margin: 20px 0; }}
        .section {{ margin: 20px 0; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{report_name} Report</h1>
        <p>Generated: {timestamp}</p>
    </div>
    
    <div class="summary">
        <h2>Summary</h2>
        <pre>{json.dumps(data.get('summary', {}), indent=2)}</pre>
    </div>
    
    <div class="section">
        <h2>Data</h2>
        <p>Routes: {len(data.get('routes', []))}</p>
        <p>Drivers: {len(data.get('drivers', []))}</p>
        <p>Vehicles: {len(data.get('vehicles', []))}</p>
        <p>Customers: {len(data.get('customers', []))}</p>
    </div>
</body>
</html>'''
        
        return html_content
    
    def export_charts_to_formats(self, chart_paths: Dict[str, str], 
                                 output_formats: List[str] = None) -> Dict[str, List[str]]:
        """
        Export charts to different formats.
        
        Args:
            chart_paths: Dictionary mapping chart names to file paths
            output_formats: List of formats to export charts to
            
        Returns:
            Dictionary mapping format names to lists of exported file paths
        """
        if output_formats is None:
            output_formats = ['png', 'svg', 'pdf']
        
        logger.info(f"Exporting charts to formats: {output_formats}")
        
        exported_charts = {}
        
        for format_name in output_formats:
            exported_charts[format_name] = []
            
            for chart_name, chart_path in chart_paths.items():
                try:
                    # Create output path
                    base_name = os.path.splitext(os.path.basename(chart_path))[0]
                    output_path = os.path.join(self.output_directory, f"{base_name}.{format_name}")
                    
                    # For now, just copy the file (in a real implementation, 
                    # you'd convert between formats)
                    import shutil
                    shutil.copy2(chart_path, output_path)
                    
                    exported_charts[format_name].append(output_path)
                    
                except Exception as e:
                    logger.error(f"Error exporting chart {chart_name} to {format_name}: {e}")
        
        return exported_charts
    
    def create_report_package(self, report_files: Dict[str, str], 
                             package_name: str = None) -> str:
        """
        Create a compressed package containing all report files.
        
        Args:
            report_files: Dictionary mapping format names to file paths
            package_name: Name for the package file
            
        Returns:
            Path to the created package file
        """
        import zipfile
        
        if package_name is None:
            package_name = f"report_package_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        package_path = os.path.join(self.output_directory, f"{package_name}.zip")
        
        with zipfile.ZipFile(package_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for format_name, file_path in report_files.items():
                if os.path.exists(file_path):
                    if os.path.isdir(file_path):
                        # Add directory contents
                        for root, dirs, files in os.walk(file_path):
                            for file in files:
                                file_full_path = os.path.join(root, file)
                                arc_name = os.path.relpath(file_full_path, self.output_directory)
                                zip_file.write(file_full_path, arc_name)
                    else:
                        # Add single file
                        arc_name = f"{format_name}_{os.path.basename(file_path)}"
                        zip_file.write(file_path, arc_name)
        
        logger.info(f"Report package created: {package_path}")
        return package_path
    
    def get_supported_formats(self) -> List[str]:
        """Get list of supported export formats."""
        formats = ['excel', 'csv', 'json', 'html']
        
        if PDF_AVAILABLE:
            formats.append('pdf')
        
        return formats
    
    def cleanup_old_files(self, days_old: int = 30):
        """Clean up old report files."""
        import time
        
        logger.info(f"Cleaning up files older than {days_old} days")
        
        cutoff_time = time.time() - (days_old * 24 * 60 * 60)
        cleaned_count = 0
        
        for filename in os.listdir(self.output_directory):
            file_path = os.path.join(self.output_directory, filename)
            
            if os.path.isfile(file_path):
                file_time = os.path.getmtime(file_path)
                
                if file_time < cutoff_time:
                    try:
                        os.remove(file_path)
                        cleaned_count += 1
                        logger.debug(f"Deleted old file: {filename}")
                    except Exception as e:
                        logger.error(f"Error deleting file {filename}: {e}")
        
        logger.info(f"Cleaned up {cleaned_count} old files")
        return cleaned_count