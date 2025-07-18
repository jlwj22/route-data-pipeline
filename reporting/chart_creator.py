"""
Chart and visualization creator for route pipeline reports.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from openpyxl.chart import LineChart, BarChart, PieChart, Reference, ScatterChart
from openpyxl.chart.axis import DateAxis
from openpyxl.chart.label import DataLabelList
from openpyxl.chart.series import Series
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import os
import base64
from io import BytesIO

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger

logger = get_logger(__name__)

class ChartCreator:
    """
    Create charts and visualizations for route pipeline reports.
    """
    
    def __init__(self):
        # Set style preferences
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
        # Chart colors
        self.color_palette = [
            '#3498db', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6',
            '#1abc9c', '#e67e22', '#34495e', '#f1c40f', '#e74c3c'
        ]
        
        # Default figure size
        self.figsize = (10, 6)
        
    def create_revenue_trend_chart(self, data: List[Dict], 
                                  output_path: str = None) -> str:
        """
        Create a revenue trend chart over time.
        """
        logger.info("Creating revenue trend chart")
        
        # Convert data to DataFrame
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['route_date'])
        
        # Group by date and sum revenue
        daily_revenue = df.groupby('date')['revenue'].sum().reset_index()
        
        # Create figure
        fig, ax = plt.subplots(figsize=self.figsize)
        
        # Plot line chart
        ax.plot(daily_revenue['date'], daily_revenue['revenue'], 
                marker='o', linewidth=2, markersize=6, color=self.color_palette[0])
        
        # Formatting
        ax.set_title('Daily Revenue Trend', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Revenue ($)', fontsize=12)
        ax.grid(True, alpha=0.3)
        
        # Format y-axis as currency
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45)
        
        # Tight layout
        plt.tight_layout()
        
        # Save chart
        if not output_path:
            output_path = 'revenue_trend.png'
        
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Revenue trend chart saved to: {output_path}")
        return output_path
    
    def create_driver_performance_chart(self, driver_metrics: Dict[str, Dict],
                                       output_path: str = None) -> str:
        """
        Create a driver performance comparison chart.
        """
        logger.info("Creating driver performance chart")
        
        # Extract data
        drivers = list(driver_metrics.keys())
        revenues = [metrics['revenue'] for metrics in driver_metrics.values()]
        miles = [metrics['total_miles'] for metrics in driver_metrics.values()]
        
        # Create figure with subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Revenue bar chart
        bars1 = ax1.bar(drivers, revenues, color=self.color_palette[:len(drivers)])
        ax1.set_title('Driver Revenue Comparison', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Driver', fontsize=12)
        ax1.set_ylabel('Revenue ($)', fontsize=12)
        ax1.tick_params(axis='x', rotation=45)
        
        # Add value labels on bars
        for bar, value in zip(bars1, revenues):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'${value:,.0f}', ha='center', va='bottom', fontsize=10)
        
        # Miles bar chart
        bars2 = ax2.bar(drivers, miles, color=self.color_palette[:len(drivers)])
        ax2.set_title('Driver Miles Comparison', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Driver', fontsize=12)
        ax2.set_ylabel('Miles', fontsize=12)
        ax2.tick_params(axis='x', rotation=45)
        
        # Add value labels on bars
        for bar, value in zip(bars2, miles):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{value:,.0f}', ha='center', va='bottom', fontsize=10)
        
        # Tight layout
        plt.tight_layout()
        
        # Save chart
        if not output_path:
            output_path = 'driver_performance.png'
        
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Driver performance chart saved to: {output_path}")
        return output_path
    
    def create_cost_breakdown_pie_chart(self, financial_metrics: Dict[str, float],
                                       output_path: str = None) -> str:
        """
        Create a pie chart showing cost breakdown.
        """
        logger.info("Creating cost breakdown pie chart")
        
        # Extract cost data
        costs = {
            'Fuel': financial_metrics.get('fuel_costs', 0),
            'Driver Pay': financial_metrics.get('driver_pay', 0),
            'Maintenance': financial_metrics.get('maintenance_costs', 0),
            'Insurance': financial_metrics.get('insurance_costs', 0),
            'Other': financial_metrics.get('other_costs', 0)
        }
        
        # Filter out zero values
        costs = {k: v for k, v in costs.items() if v > 0}
        
        # Create figure
        fig, ax = plt.subplots(figsize=(8, 8))
        
        # Create pie chart
        wedges, texts, autotexts = ax.pie(costs.values(), labels=costs.keys(), 
                                         autopct='%1.1f%%', startangle=90,
                                         colors=self.color_palette[:len(costs)])
        
        # Formatting
        ax.set_title('Cost Breakdown', fontsize=16, fontweight='bold', pad=20)
        
        # Make percentage text more readable
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        # Equal aspect ratio ensures that pie is drawn as a circle
        ax.axis('equal')
        
        # Add legend
        ax.legend(wedges, [f'{label}: ${value:,.0f}' for label, value in costs.items()],
                 title="Cost Categories", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
        
        # Tight layout
        plt.tight_layout()
        
        # Save chart
        if not output_path:
            output_path = 'cost_breakdown.png'
        
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Cost breakdown pie chart saved to: {output_path}")
        return output_path
    
    def create_efficiency_scatter_plot(self, data: List[Dict],
                                      output_path: str = None) -> str:
        """
        Create a scatter plot showing efficiency vs revenue.
        """
        logger.info("Creating efficiency scatter plot")
        
        # Convert data to DataFrame
        df = pd.DataFrame(data)
        
        # Create figure
        fig, ax = plt.subplots(figsize=self.figsize)
        
        # Create scatter plot
        scatter = ax.scatter(df['efficiency_score'], df['revenue'], 
                           c=df['total_miles'], cmap='viridis', 
                           alpha=0.6, s=60)
        
        # Add colorbar
        cbar = plt.colorbar(scatter)
        cbar.set_label('Total Miles', fontsize=12)
        
        # Formatting
        ax.set_title('Route Efficiency vs Revenue', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Efficiency Score', fontsize=12)
        ax.set_ylabel('Revenue ($)', fontsize=12)
        ax.grid(True, alpha=0.3)
        
        # Format y-axis as currency
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Add trend line
        if len(df) > 1:
            z = np.polyfit(df['efficiency_score'], df['revenue'], 1)
            p = np.poly1d(z)
            ax.plot(df['efficiency_score'], p(df['efficiency_score']), 
                   "r--", alpha=0.8, linewidth=2, label='Trend Line')
            ax.legend()
        
        # Tight layout
        plt.tight_layout()
        
        # Save chart
        if not output_path:
            output_path = 'efficiency_scatter.png'
        
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Efficiency scatter plot saved to: {output_path}")
        return output_path
    
    def create_monthly_comparison_chart(self, monthly_data: Dict[str, Dict],
                                       output_path: str = None) -> str:
        """
        Create a monthly comparison chart.
        """
        logger.info("Creating monthly comparison chart")
        
        # Extract data
        months = list(monthly_data.keys())
        revenues = [data['revenue'] for data in monthly_data.values()]
        costs = [data['costs'] for data in monthly_data.values()]
        profits = [data['profit'] for data in monthly_data.values()]
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Set width of bars
        bar_width = 0.25
        x = range(len(months))
        
        # Create bars
        bars1 = ax.bar([i - bar_width for i in x], revenues, bar_width, 
                      label='Revenue', color=self.color_palette[0])
        bars2 = ax.bar([i for i in x], costs, bar_width, 
                      label='Costs', color=self.color_palette[1])
        bars3 = ax.bar([i + bar_width for i in x], profits, bar_width, 
                      label='Profit', color=self.color_palette[2])
        
        # Formatting
        ax.set_title('Monthly Financial Comparison', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Month', fontsize=12)
        ax.set_ylabel('Amount ($)', fontsize=12)
        ax.set_xticks(x)
        ax.set_xticklabels(months)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Format y-axis as currency
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Tight layout
        plt.tight_layout()
        
        # Save chart
        if not output_path:
            output_path = 'monthly_comparison.png'
        
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Monthly comparison chart saved to: {output_path}")
        return output_path
    
    def create_fuel_efficiency_chart(self, vehicle_data: Dict[str, Dict],
                                    output_path: str = None) -> str:
        """
        Create a fuel efficiency comparison chart.
        """
        logger.info("Creating fuel efficiency chart")
        
        # Extract data
        vehicles = list(vehicle_data.keys())
        mpg_values = [data['mpg'] for data in vehicle_data.values()]
        
        # Create figure
        fig, ax = plt.subplots(figsize=self.figsize)
        
        # Create horizontal bar chart
        bars = ax.barh(vehicles, mpg_values, color=self.color_palette[:len(vehicles)])
        
        # Formatting
        ax.set_title('Vehicle Fuel Efficiency Comparison', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Miles Per Gallon (MPG)', fontsize=12)
        ax.set_ylabel('Vehicle', fontsize=12)
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add value labels on bars
        for bar, value in zip(bars, mpg_values):
            width = bar.get_width()
            ax.text(width + 0.1, bar.get_y() + bar.get_height()/2.,
                   f'{value:.1f}', ha='left', va='center', fontsize=10)
        
        # Tight layout
        plt.tight_layout()
        
        # Save chart
        if not output_path:
            output_path = 'fuel_efficiency.png'
        
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Fuel efficiency chart saved to: {output_path}")
        return output_path
    
    def create_excel_chart(self, worksheet, chart_type: str, data_range: str,
                          title: str, position: str = 'G2') -> None:
        """
        Create a chart in an Excel worksheet.
        """
        logger.info(f"Creating {chart_type} chart in Excel worksheet")
        
        # Create chart based on type
        if chart_type.lower() == 'line':
            chart = LineChart()
        elif chart_type.lower() == 'bar':
            chart = BarChart()
        elif chart_type.lower() == 'pie':
            chart = PieChart()
        elif chart_type.lower() == 'scatter':
            chart = ScatterChart()
        else:
            logger.warning(f"Unknown chart type: {chart_type}")
            return
        
        # Set title
        chart.title = title
        
        # Create data reference
        data = Reference(worksheet, range_string=data_range)
        chart.add_data(data, titles_from_data=True)
        
        # Add chart to worksheet
        worksheet.add_chart(chart, position)
        
        logger.info(f"Excel chart added to worksheet at position {position}")
    
    def create_dashboard_charts(self, data: List[Dict], output_dir: str) -> Dict[str, str]:
        """
        Create a complete set of dashboard charts.
        """
        logger.info("Creating complete dashboard charts")
        
        charts = {}
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Revenue trend
        charts['revenue_trend'] = self.create_revenue_trend_chart(
            data, os.path.join(output_dir, 'revenue_trend.png')
        )
        
        # Driver performance
        driver_metrics = self._calculate_driver_metrics(data)
        charts['driver_performance'] = self.create_driver_performance_chart(
            driver_metrics, os.path.join(output_dir, 'driver_performance.png')
        )
        
        # Cost breakdown
        financial_metrics = self._calculate_financial_metrics(data)
        charts['cost_breakdown'] = self.create_cost_breakdown_pie_chart(
            financial_metrics, os.path.join(output_dir, 'cost_breakdown.png')
        )
        
        # Efficiency scatter plot
        charts['efficiency_scatter'] = self.create_efficiency_scatter_plot(
            data, os.path.join(output_dir, 'efficiency_scatter.png')
        )
        
        # Fuel efficiency
        vehicle_data = self._calculate_vehicle_metrics(data)
        charts['fuel_efficiency'] = self.create_fuel_efficiency_chart(
            vehicle_data, os.path.join(output_dir, 'fuel_efficiency.png')
        )
        
        logger.info(f"Dashboard charts created in: {output_dir}")
        return charts
    
    def _calculate_driver_metrics(self, data: List[Dict]) -> Dict[str, Dict]:
        """Calculate driver metrics from route data."""
        driver_metrics = {}
        
        for route in data:
            driver = route.get('driver_name', 'Unknown')
            
            if driver not in driver_metrics:
                driver_metrics[driver] = {
                    'revenue': 0,
                    'total_miles': 0,
                    'route_count': 0
                }
            
            driver_metrics[driver]['revenue'] += route.get('revenue', 0)
            driver_metrics[driver]['total_miles'] += route.get('total_miles', 0)
            driver_metrics[driver]['route_count'] += 1
        
        return driver_metrics
    
    def _calculate_financial_metrics(self, data: List[Dict]) -> Dict[str, float]:
        """Calculate financial metrics from route data."""
        metrics = {
            'fuel_costs': 0,
            'driver_pay': 0,
            'maintenance_costs': 0,
            'insurance_costs': 0,
            'other_costs': 0
        }
        
        for route in data:
            metrics['fuel_costs'] += route.get('fuel_cost', 0)
            metrics['driver_pay'] += route.get('driver_pay', 0)
            metrics['other_costs'] += route.get('other_costs', 0)
            
            # Estimate maintenance and insurance costs
            miles = route.get('total_miles', 0)
            metrics['maintenance_costs'] += miles * 0.08  # $0.08 per mile
            metrics['insurance_costs'] += miles * 0.05    # $0.05 per mile
        
        return metrics
    
    def _calculate_vehicle_metrics(self, data: List[Dict]) -> Dict[str, Dict]:
        """Calculate vehicle metrics from route data."""
        vehicle_metrics = {}
        
        for route in data:
            vehicle = route.get('vehicle_info', 'Unknown')
            
            if vehicle not in vehicle_metrics:
                vehicle_metrics[vehicle] = {
                    'total_miles': 0,
                    'fuel_used': 0,
                    'mpg': 0
                }
            
            vehicle_metrics[vehicle]['total_miles'] += route.get('total_miles', 0)
            vehicle_metrics[vehicle]['fuel_used'] += route.get('fuel_consumed', 0)
        
        # Calculate MPG
        for vehicle, metrics in vehicle_metrics.items():
            if metrics['fuel_used'] > 0:
                metrics['mpg'] = metrics['total_miles'] / metrics['fuel_used']
        
        return vehicle_metrics
    
    def save_chart_as_base64(self, chart_path: str) -> str:
        """
        Convert a chart image to base64 string for embedding.
        """
        try:
            with open(chart_path, 'rb') as image_file:
                encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
            return encoded_string
        except Exception as e:
            logger.error(f"Error converting chart to base64: {e}")
            return ""
    
    def create_chart_from_dataframe(self, df: pd.DataFrame, chart_type: str,
                                   x_column: str, y_column: str,
                                   title: str, output_path: str) -> str:
        """
        Create a chart from a pandas DataFrame.
        """
        logger.info(f"Creating {chart_type} chart from DataFrame")
        
        # Create figure
        fig, ax = plt.subplots(figsize=self.figsize)
        
        if chart_type.lower() == 'line':
            ax.plot(df[x_column], df[y_column], marker='o', linewidth=2, markersize=6)
        elif chart_type.lower() == 'bar':
            ax.bar(df[x_column], df[y_column])
        elif chart_type.lower() == 'scatter':
            ax.scatter(df[x_column], df[y_column], alpha=0.6)
        else:
            logger.warning(f"Unsupported chart type: {chart_type}")
            return None
        
        # Formatting
        ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel(x_column.replace('_', ' ').title(), fontsize=12)
        ax.set_ylabel(y_column.replace('_', ' ').title(), fontsize=12)
        ax.grid(True, alpha=0.3)
        
        # Tight layout
        plt.tight_layout()
        
        # Save chart
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Chart saved to: {output_path}")
        return output_path