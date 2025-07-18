"""
Main report manager that orchestrates all reporting functionality.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from openpyxl import Workbook

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .excel_generator import ExcelReportGenerator
from .chart_creator import ChartCreator
from .template_manager import TemplateManager
from .multi_format_exporter import MultiFormatExporter
from database.operations import DatabaseOperations
from config.settings import Settings
from utils.logger import get_logger

logger = get_logger(__name__)

class ReportManager:
    """
    Main report manager that orchestrates all reporting functionality.
    """
    
    def __init__(self, settings: Settings, db_operations: DatabaseOperations):
        self.settings = settings
        self.db_ops = db_operations
        
        # Initialize components
        self.excel_generator = ExcelReportGenerator(settings, db_operations)
        self.chart_creator = ChartCreator()
        self.template_manager = TemplateManager()
        self.multi_format_exporter = MultiFormatExporter(settings.output_directory)
        
        logger.info("Report manager initialized")
    
    def generate_daily_report(self, date: datetime, 
                             template_name: str = "Daily Route Summary",
                             formats: List[str] = None,
                             include_charts: bool = True) -> Dict[str, Any]:
        """
        Generate a complete daily report with charts and multiple formats.
        """
        logger.info(f"Generating daily report for {date.strftime('%Y-%m-%d')}")
        
        # Get data for the date
        routes_data = self._get_routes_data(date, date)
        
        if not routes_data:
            logger.warning(f"No route data found for {date.strftime('%Y-%m-%d')}")
            return {'status': 'no_data', 'date': date.strftime('%Y-%m-%d')}
        
        # Prepare report data
        report_data = self._prepare_report_data(routes_data, 'daily')
        
        # Generate Excel report using template
        excel_path = self._generate_excel_with_template(
            report_data, template_name, f"daily_report_{date.strftime('%Y%m%d')}"
        )
        
        # Generate charts
        charts = {}
        if include_charts:
            charts = self._generate_report_charts(report_data, 'daily')
        
        # Export to multiple formats
        exported_files = {}
        if formats:
            exported_files = self.multi_format_exporter.export_report(
                report_data, 
                f"daily_report_{date.strftime('%Y%m%d')}", 
                formats,
                {
                    'report_type': 'daily',
                    'report_date': date.strftime('%Y-%m-%d'),
                    'generated_at': datetime.now().isoformat()
                }
            )
        
        return {
            'status': 'success',
            'date': date.strftime('%Y-%m-%d'),
            'excel_path': excel_path,
            'charts': charts,
            'exported_files': exported_files,
            'data_summary': {
                'routes_count': len(routes_data),
                'total_revenue': sum(route.get('revenue', 0) for route in routes_data),
                'total_miles': sum(route.get('total_miles', 0) for route in routes_data)
            }
        }
    
    def generate_weekly_report(self, start_date: datetime,
                              template_name: str = "Weekly Performance Report",
                              formats: List[str] = None,
                              include_charts: bool = True) -> Dict[str, Any]:
        """
        Generate a complete weekly report.
        """
        end_date = start_date + timedelta(days=6)
        logger.info(f"Generating weekly report from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Get data for the week
        routes_data = self._get_routes_data(start_date, end_date)
        
        if not routes_data:
            logger.warning(f"No route data found for week {start_date.strftime('%Y-%m-%d')}")
            return {'status': 'no_data', 'start_date': start_date.strftime('%Y-%m-%d')}
        
        # Prepare report data
        report_data = self._prepare_report_data(routes_data, 'weekly')
        
        # Generate Excel report using template
        excel_path = self._generate_excel_with_template(
            report_data, template_name, f"weekly_report_{start_date.strftime('%Y%m%d')}"
        )
        
        # Generate charts
        charts = {}
        if include_charts:
            charts = self._generate_report_charts(report_data, 'weekly')
        
        # Export to multiple formats
        exported_files = {}
        if formats:
            exported_files = self.multi_format_exporter.export_report(
                report_data,
                f"weekly_report_{start_date.strftime('%Y%m%d')}",
                formats,
                {
                    'report_type': 'weekly',
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'generated_at': datetime.now().isoformat()
                }
            )
        
        return {
            'status': 'success',
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'excel_path': excel_path,
            'charts': charts,
            'exported_files': exported_files,
            'data_summary': {
                'routes_count': len(routes_data),
                'total_revenue': sum(route.get('revenue', 0) for route in routes_data),
                'total_miles': sum(route.get('total_miles', 0) for route in routes_data)
            }
        }
    
    def generate_monthly_report(self, year: int, month: int,
                               template_name: str = "Monthly Comprehensive Report",
                               formats: List[str] = None,
                               include_charts: bool = True) -> Dict[str, Any]:
        """
        Generate a complete monthly report.
        """
        logger.info(f"Generating monthly report for {year}-{month:02d}")
        
        # Calculate date range
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        
        # Get data for the month
        routes_data = self._get_routes_data(start_date, end_date)
        
        if not routes_data:
            logger.warning(f"No route data found for {year}-{month:02d}")
            return {'status': 'no_data', 'year': year, 'month': month}
        
        # Prepare report data
        report_data = self._prepare_report_data(routes_data, 'monthly')
        
        # Generate Excel report using template
        excel_path = self._generate_excel_with_template(
            report_data, template_name, f"monthly_report_{year}_{month:02d}"
        )
        
        # Generate charts
        charts = {}
        if include_charts:
            charts = self._generate_report_charts(report_data, 'monthly')
        
        # Export to multiple formats
        exported_files = {}
        if formats:
            exported_files = self.multi_format_exporter.export_report(
                report_data,
                f"monthly_report_{year}_{month:02d}",
                formats,
                {
                    'report_type': 'monthly',
                    'year': year,
                    'month': month,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'generated_at': datetime.now().isoformat()
                }
            )
        
        return {
            'status': 'success',
            'year': year,
            'month': month,
            'excel_path': excel_path,
            'charts': charts,
            'exported_files': exported_files,
            'data_summary': {
                'routes_count': len(routes_data),
                'total_revenue': sum(route.get('revenue', 0) for route in routes_data),
                'total_miles': sum(route.get('total_miles', 0) for route in routes_data)
            }
        }
    
    def generate_custom_report(self, start_date: datetime, end_date: datetime,
                              report_name: str, template_name: str = None,
                              formats: List[str] = None,
                              include_charts: bool = True) -> Dict[str, Any]:
        """
        Generate a custom report for any date range.
        """
        logger.info(f"Generating custom report '{report_name}' from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Get data for the date range
        routes_data = self._get_routes_data(start_date, end_date)
        
        if not routes_data:
            logger.warning(f"No route data found for date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            return {'status': 'no_data', 'start_date': start_date.strftime('%Y-%m-%d'), 'end_date': end_date.strftime('%Y-%m-%d')}
        
        # Prepare report data
        report_data = self._prepare_report_data(routes_data, 'custom')
        
        # Generate Excel report
        if template_name:
            excel_path = self._generate_excel_with_template(
                report_data, template_name, report_name
            )
        else:
            excel_path = self._generate_basic_excel_report(
                report_data, report_name
            )
        
        # Generate charts
        charts = {}
        if include_charts:
            charts = self._generate_report_charts(report_data, 'custom')
        
        # Export to multiple formats
        exported_files = {}
        if formats:
            exported_files = self.multi_format_exporter.export_report(
                report_data,
                report_name,
                formats,
                {
                    'report_type': 'custom',
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'generated_at': datetime.now().isoformat()
                }
            )
        
        return {
            'status': 'success',
            'report_name': report_name,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'excel_path': excel_path,
            'charts': charts,
            'exported_files': exported_files,
            'data_summary': {
                'routes_count': len(routes_data),
                'total_revenue': sum(route.get('revenue', 0) for route in routes_data),
                'total_miles': sum(route.get('total_miles', 0) for route in routes_data)
            }
        }
    
    def _get_routes_data(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get routes data for a date range."""
        try:
            # This would call the database operations to get route data
            # For now, return sample data structure
            return self.db_ops.get_routes_by_date_range(start_date, end_date)
        except Exception as e:
            logger.error(f"Error getting routes data: {e}")
            return []
    
    def _prepare_report_data(self, routes_data: List[Dict[str, Any]], 
                           report_type: str) -> Dict[str, Any]:
        """Prepare data for report generation."""
        
        # Calculate summary metrics
        summary = {
            'total_routes': len(routes_data),
            'total_miles': sum(route.get('total_miles', 0) for route in routes_data),
            'total_revenue': sum(route.get('revenue', 0) for route in routes_data),
            'total_costs': sum(route.get('total_costs', 0) for route in routes_data),
            'average_miles_per_route': 0,
            'average_revenue_per_route': 0,
        }
        
        if summary['total_routes'] > 0:
            summary['average_miles_per_route'] = summary['total_miles'] / summary['total_routes']
            summary['average_revenue_per_route'] = summary['total_revenue'] / summary['total_routes']
        
        summary['total_profit'] = summary['total_revenue'] - summary['total_costs']
        summary['profit_margin'] = (summary['total_profit'] / summary['total_revenue'] * 100) if summary['total_revenue'] > 0 else 0
        
        # Calculate driver metrics
        driver_metrics = self._calculate_driver_metrics(routes_data)
        
        # Calculate vehicle metrics
        vehicle_metrics = self._calculate_vehicle_metrics(routes_data)
        
        # Calculate customer metrics
        customer_metrics = self._calculate_customer_metrics(routes_data)
        
        return {
            'routes': routes_data,
            'summary': summary,
            'drivers': driver_metrics,
            'vehicles': vehicle_metrics,
            'customers': customer_metrics,
            'report_type': report_type,
            'report_date': datetime.now().strftime('%Y-%m-%d')
        }
    
    def _calculate_driver_metrics(self, routes_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Calculate driver performance metrics."""
        driver_metrics = {}
        
        for route in routes_data:
            driver_name = route.get('driver_name', 'Unknown')
            
            if driver_name not in driver_metrics:
                driver_metrics[driver_name] = {
                    'name': driver_name,
                    'route_count': 0,
                    'total_miles': 0,
                    'revenue': 0,
                    'efficiency_scores': [],
                    'speeds': []
                }
            
            metrics = driver_metrics[driver_name]
            metrics['route_count'] += 1
            metrics['total_miles'] += route.get('total_miles', 0)
            metrics['revenue'] += route.get('revenue', 0)
            
            if route.get('efficiency_score'):
                metrics['efficiency_scores'].append(route['efficiency_score'])
            
            if route.get('average_speed'):
                metrics['speeds'].append(route['average_speed'])
        
        # Calculate averages
        driver_list = []
        for driver, metrics in driver_metrics.items():
            metrics['efficiency'] = sum(metrics['efficiency_scores']) / len(metrics['efficiency_scores']) if metrics['efficiency_scores'] else 0
            metrics['avg_speed'] = sum(metrics['speeds']) / len(metrics['speeds']) if metrics['speeds'] else 0
            
            # Remove temporary lists
            del metrics['efficiency_scores']
            del metrics['speeds']
            
            driver_list.append(metrics)
        
        return driver_list
    
    def _calculate_vehicle_metrics(self, routes_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Calculate vehicle performance metrics."""
        vehicle_metrics = {}
        
        for route in routes_data:
            vehicle_info = route.get('vehicle_info', 'Unknown')
            
            if vehicle_info not in vehicle_metrics:
                vehicle_metrics[vehicle_info] = {
                    'vehicle_info': vehicle_info,
                    'route_count': 0,
                    'total_miles': 0,
                    'fuel_used': 0,
                    'maintenance_cost': 0
                }
            
            metrics = vehicle_metrics[vehicle_info]
            metrics['route_count'] += 1
            metrics['total_miles'] += route.get('total_miles', 0)
            metrics['fuel_used'] += route.get('fuel_consumed', 0)
            metrics['maintenance_cost'] += route.get('maintenance_cost', 0)
        
        # Calculate derived metrics
        vehicle_list = []
        for vehicle, metrics in vehicle_metrics.items():
            metrics['mpg'] = metrics['total_miles'] / metrics['fuel_used'] if metrics['fuel_used'] > 0 else 0
            metrics['cost_per_mile'] = metrics['maintenance_cost'] / metrics['total_miles'] if metrics['total_miles'] > 0 else 0
            
            vehicle_list.append(metrics)
        
        return vehicle_list
    
    def _calculate_customer_metrics(self, routes_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Calculate customer metrics."""
        customer_metrics = {}
        
        for route in routes_data:
            customer_name = route.get('customer_name', 'Unknown')
            
            if customer_name not in customer_metrics:
                customer_metrics[customer_name] = {
                    'customer_name': customer_name,
                    'route_count': 0,
                    'revenue': 0,
                    'total_miles': 0
                }
            
            metrics = customer_metrics[customer_name]
            metrics['route_count'] += 1
            metrics['revenue'] += route.get('revenue', 0)
            metrics['total_miles'] += route.get('total_miles', 0)
        
        # Calculate derived metrics
        customer_list = []
        for customer, metrics in customer_metrics.items():
            metrics['avg_revenue_per_route'] = metrics['revenue'] / metrics['route_count'] if metrics['route_count'] > 0 else 0
            metrics['revenue_per_mile'] = metrics['revenue'] / metrics['total_miles'] if metrics['total_miles'] > 0 else 0
            
            customer_list.append(metrics)
        
        return customer_list
    
    def _generate_excel_with_template(self, report_data: Dict[str, Any], 
                                    template_name: str, report_name: str) -> Optional[str]:
        """Generate Excel report using a template."""
        try:
            wb = Workbook()
            
            # Apply template
            wb = self.template_manager.apply_template(template_name, report_data, wb)
            
            # Save workbook
            output_path = os.path.join(self.settings.output_directory, f"{report_name}.xlsx")
            wb.save(output_path)
            
            logger.info(f"Excel report with template saved: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating Excel report with template: {e}")
            return None
    
    def _generate_basic_excel_report(self, report_data: Dict[str, Any], 
                                   report_name: str) -> Optional[str]:
        """Generate basic Excel report without template."""
        try:
            output_path = os.path.join(self.settings.output_directory, f"{report_name}.xlsx")
            
            # Use the excel generator directly
            wb = Workbook()
            # Basic implementation would go here
            wb.save(output_path)
            
            logger.info(f"Basic Excel report saved: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating basic Excel report: {e}")
            return None
    
    def _generate_report_charts(self, report_data: Dict[str, Any], 
                              report_type: str) -> Dict[str, str]:
        """Generate charts for the report."""
        try:
            charts_dir = os.path.join(self.settings.output_directory, 'charts')
            os.makedirs(charts_dir, exist_ok=True)
            
            # Generate charts using the chart creator
            charts = self.chart_creator.create_dashboard_charts(
                report_data.get('routes', []), 
                charts_dir
            )
            
            logger.info(f"Generated {len(charts)} charts for {report_type} report")
            return charts
            
        except Exception as e:
            logger.error(f"Error generating charts: {e}")
            return {}
    
    def list_available_templates(self) -> List[str]:
        """List all available report templates."""
        return self.template_manager.list_templates()
    
    def get_supported_export_formats(self) -> List[str]:
        """Get list of supported export formats."""
        return self.multi_format_exporter.get_supported_formats()
    
    def cleanup_old_reports(self, days_old: int = 30) -> int:
        """Clean up old report files."""
        return self.multi_format_exporter.cleanup_old_files(days_old)
    
    def get_report_statistics(self) -> Dict[str, Any]:
        """Get statistics about generated reports."""
        output_dir = self.settings.output_directory
        
        if not os.path.exists(output_dir):
            return {'total_files': 0, 'total_size': 0}
        
        total_files = 0
        total_size = 0
        
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.isfile(file_path):
                    total_files += 1
                    total_size += os.path.getsize(file_path)
        
        return {
            'total_files': total_files,
            'total_size': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'output_directory': output_dir
        }