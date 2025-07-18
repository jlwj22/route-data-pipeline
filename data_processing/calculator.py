from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
import math
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger

logger = get_logger(__name__)

class RouteCalculator:
    """
    Business calculations for route data processing.
    Handles distance, cost, efficiency, and performance calculations.
    """
    
    def __init__(self):
        # Default rates - should be configurable
        self.default_fuel_price = 3.50  # per gallon
        self.default_toll_rate = 0.15   # per mile
        self.default_maintenance_rate = 0.08  # per mile
        self.default_insurance_rate = 0.05    # per mile
        
    def calculate_distance(self, lat1: float, lon1: float, 
                          lat2: float, lon2: float) -> float:
        """
        Calculate distance between two points using Haversine formula.
        Returns distance in miles.
        """
        try:
            # Convert latitude and longitude from degrees to radians
            lat1_rad = math.radians(lat1)
            lon1_rad = math.radians(lon1)
            lat2_rad = math.radians(lat2)
            lon2_rad = math.radians(lon2)
            
            # Haversine formula
            dlat = lat2_rad - lat1_rad
            dlon = lon2_rad - lon1_rad
            
            a = (math.sin(dlat/2)**2 + 
                 math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2)
            c = 2 * math.asin(math.sqrt(a))
            
            # Radius of earth in miles
            r = 3956
            
            distance = c * r
            return round(distance, 2)
            
        except Exception as e:
            logger.error(f"Error calculating distance: {e}")
            return 0.0
    
    def calculate_fuel_cost(self, miles: float, mpg: float, 
                           fuel_price: Optional[float] = None) -> float:
        """
        Calculate fuel cost for a trip.
        """
        if not miles or not mpg or mpg <= 0:
            return 0.0
            
        fuel_price = fuel_price or self.default_fuel_price
        gallons_used = miles / mpg
        return round(gallons_used * fuel_price, 2)
    
    def calculate_toll_cost(self, miles: float, 
                           toll_rate: Optional[float] = None) -> float:
        """
        Calculate estimated toll cost based on miles.
        """
        if not miles:
            return 0.0
            
        toll_rate = toll_rate or self.default_toll_rate
        return round(miles * toll_rate, 2)
    
    def calculate_driver_pay(self, hours: float, hourly_rate: float,
                            miles: float = 0, mile_rate: float = 0) -> float:
        """
        Calculate driver pay based on hours and/or miles.
        """
        hourly_pay = hours * hourly_rate if hours and hourly_rate else 0
        mile_pay = miles * mile_rate if miles and mile_rate else 0
        
        # Use the higher of the two
        return round(max(hourly_pay, mile_pay), 2)
    
    def calculate_trip_duration(self, start_time, end_time) -> Dict[str, float]:
        """
        Calculate various time metrics for a trip.
        """
        if not start_time or not end_time:
            return {
                'total_hours': 0.0,
                'total_minutes': 0.0,
                'driving_hours': 0.0,
                'wait_hours': 0.0
            }
        
        # Convert strings to datetime if needed
        if isinstance(start_time, str):
            from datetime import datetime
            try:
                start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except ValueError:
                # Try common formats
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d']:
                    try:
                        start_time = datetime.strptime(start_time, fmt)
                        break
                    except ValueError:
                        continue
        if isinstance(end_time, str):
            from datetime import datetime
            try:
                end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            except ValueError:
                # Try common formats
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d']:
                    try:
                        end_time = datetime.strptime(end_time, fmt)
                        break
                    except ValueError:
                        continue
        
        duration = end_time - start_time
        total_hours = duration.total_seconds() / 3600
        total_minutes = duration.total_seconds() / 60
        
        return {
            'total_hours': round(total_hours, 2),
            'total_minutes': round(total_minutes, 2),
            'driving_hours': round(total_hours * 0.8, 2),  # Estimate 80% driving
            'wait_hours': round(total_hours * 0.2, 2)      # Estimate 20% waiting
        }
    
    def calculate_average_speed(self, miles: float, hours: float) -> float:
        """
        Calculate average speed for a trip.
        """
        if not miles or not hours or hours <= 0:
            return 0.0
            
        return round(miles / hours, 2)
    
    def calculate_efficiency_score(self, actual_miles: float, 
                                  optimal_miles: float,
                                  actual_time: float,
                                  scheduled_time: float) -> float:
        """
        Calculate route efficiency score (0-100).
        """
        if not actual_miles or not optimal_miles or not actual_time or not scheduled_time:
            return 0.0
        
        # Distance efficiency (lower is better)
        distance_ratio = optimal_miles / actual_miles if actual_miles > 0 else 0
        distance_score = min(distance_ratio * 100, 100)
        
        # Time efficiency (on-time or early is better)
        time_ratio = scheduled_time / actual_time if actual_time > 0 else 0
        time_score = min(time_ratio * 100, 100)
        
        # Combined score (weighted average)
        efficiency_score = (distance_score * 0.6) + (time_score * 0.4)
        return round(efficiency_score, 2)
    
    def calculate_profit_margin(self, revenue: float, total_costs: float) -> float:
        """
        Calculate profit margin as a percentage.
        """
        if not revenue or revenue <= 0:
            return 0.0
            
        profit = revenue - total_costs
        margin = (profit / revenue) * 100
        return round(margin, 2)
    
    def calculate_cost_per_mile(self, total_costs: float, miles: float) -> float:
        """
        Calculate cost per mile.
        """
        if not miles or miles <= 0:
            return 0.0
            
        return round(total_costs / miles, 2)
    
    def calculate_revenue_per_mile(self, revenue: float, miles: float) -> float:
        """
        Calculate revenue per mile.
        """
        if not miles or miles <= 0:
            return 0.0
            
        return round(revenue / miles, 2)
    
    def calculate_deadhead_percentage(self, empty_miles: float, 
                                    total_miles: float) -> float:
        """
        Calculate deadhead (empty miles) percentage.
        """
        if not total_miles or total_miles <= 0:
            return 0.0
            
        return round((empty_miles / total_miles) * 100, 2)
    
    def calculate_fuel_efficiency(self, miles: float, 
                                 fuel_consumed: float) -> float:
        """
        Calculate miles per gallon.
        """
        if not fuel_consumed or fuel_consumed <= 0:
            return 0.0
            
        return round(miles / fuel_consumed, 2)
    
    def calculate_total_costs(self, fuel_cost: float = 0, toll_cost: float = 0,
                             driver_pay: float = 0, other_costs: float = 0,
                             miles: float = 0) -> Dict[str, float]:
        """
        Calculate total costs and breakdown.
        """
        # Add estimated maintenance and insurance costs
        maintenance_cost = miles * self.default_maintenance_rate
        insurance_cost = miles * self.default_insurance_rate
        
        total_variable_costs = fuel_cost + toll_cost + maintenance_cost + insurance_cost
        total_costs = total_variable_costs + driver_pay + other_costs
        
        return {
            'fuel_cost': round(fuel_cost, 2),
            'toll_cost': round(toll_cost, 2),
            'driver_pay': round(driver_pay, 2),
            'maintenance_cost': round(maintenance_cost, 2),
            'insurance_cost': round(insurance_cost, 2),
            'other_costs': round(other_costs, 2),
            'total_variable_costs': round(total_variable_costs, 2),
            'total_costs': round(total_costs, 2)
        }
    
    def calculate_route_metrics(self, route_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate all metrics for a route.
        """
        metrics = {}
        
        # Extract basic data
        total_miles = route_data.get('total_miles', 0) or 0
        empty_miles = route_data.get('empty_miles', 0) or 0
        fuel_consumed = route_data.get('fuel_consumed', 0) or 0
        revenue = route_data.get('revenue', 0) or 0
        
        # Time calculations
        start_time = route_data.get('actual_start_time') or route_data.get('scheduled_start_time')
        end_time = route_data.get('actual_end_time') or route_data.get('scheduled_end_time')
        
        if start_time and end_time:
            time_metrics = self.calculate_trip_duration(start_time, end_time)
            metrics.update(time_metrics)
            
            # Calculate average speed
            if total_miles and time_metrics['total_hours']:
                metrics['average_speed'] = self.calculate_average_speed(
                    total_miles, time_metrics['total_hours']
                )
        
        # Distance calculations
        start_location = route_data.get('start_location', {})
        end_location = route_data.get('end_location', {})
        
        if (start_location.get('latitude') and start_location.get('longitude') and
            end_location.get('latitude') and end_location.get('longitude')):
            
            calculated_distance = self.calculate_distance(
                start_location['latitude'], start_location['longitude'],
                end_location['latitude'], end_location['longitude']
            )
            metrics['calculated_distance'] = calculated_distance
            
            # Efficiency score
            if total_miles:
                metrics['distance_efficiency'] = round(
                    (calculated_distance / total_miles) * 100, 2
                )
        
        # Cost calculations
        vehicle_data = route_data.get('vehicle', {})
        mpg = vehicle_data.get('mpg_average', 8.0)  # Default truck MPG
        
        fuel_cost = self.calculate_fuel_cost(total_miles, mpg)
        toll_cost = self.calculate_toll_cost(total_miles)
        
        driver_data = route_data.get('driver', {})
        hourly_rate = driver_data.get('hourly_rate', 25.0)  # Default rate
        
        driver_pay = 0
        if 'total_hours' in metrics:
            driver_pay = self.calculate_driver_pay(
                metrics['total_hours'], hourly_rate
            )
        
        other_costs = route_data.get('other_costs', 0) or 0
        
        cost_breakdown = self.calculate_total_costs(
            fuel_cost, toll_cost, driver_pay, other_costs, total_miles
        )
        metrics.update(cost_breakdown)
        
        # Revenue calculations
        if revenue:
            metrics['profit'] = round(revenue - cost_breakdown['total_costs'], 2)
            metrics['profit_margin'] = self.calculate_profit_margin(
                revenue, cost_breakdown['total_costs']
            )
            
            if total_miles:
                metrics['revenue_per_mile'] = self.calculate_revenue_per_mile(
                    revenue, total_miles
                )
                metrics['cost_per_mile'] = self.calculate_cost_per_mile(
                    cost_breakdown['total_costs'], total_miles
                )
        
        # Efficiency metrics
        if total_miles:
            metrics['deadhead_percentage'] = self.calculate_deadhead_percentage(
                empty_miles, total_miles
            )
            
            if fuel_consumed:
                metrics['fuel_efficiency'] = self.calculate_fuel_efficiency(
                    total_miles, fuel_consumed
                )
        
        # Overall efficiency score
        scheduled_time = None
        actual_time = None
        
        if (route_data.get('scheduled_start_time') and 
            route_data.get('scheduled_end_time')):
            scheduled_duration = (route_data['scheduled_end_time'] - 
                                route_data['scheduled_start_time'])
            scheduled_time = scheduled_duration.total_seconds() / 3600
        
        if 'total_hours' in metrics:
            actual_time = metrics['total_hours']
        
        if (metrics.get('calculated_distance') and total_miles and 
            scheduled_time and actual_time):
            metrics['efficiency_score'] = self.calculate_efficiency_score(
                total_miles, metrics['calculated_distance'],
                actual_time, scheduled_time
            )
        
        return metrics
    
    def calculate_driver_performance(self, routes: list) -> Dict[str, Any]:
        """
        Calculate performance metrics for a driver across multiple routes.
        """
        if not routes:
            return {}
        
        total_miles = sum(route.get('total_miles', 0) or 0 for route in routes)
        total_revenue = sum(route.get('revenue', 0) or 0 for route in routes)
        total_costs = sum(route.get('total_costs', 0) or 0 for route in routes)
        
        # Calculate averages
        avg_efficiency = sum(
            route.get('efficiency_score', 0) or 0 for route in routes
        ) / len(routes)
        
        avg_speed = sum(
            route.get('average_speed', 0) or 0 for route in routes
        ) / len(routes)
        
        # On-time performance
        on_time_count = sum(
            1 for route in routes 
            if route.get('efficiency_score', 0) >= 80
        )
        on_time_percentage = (on_time_count / len(routes)) * 100
        
        return {
            'total_routes': len(routes),
            'total_miles': round(total_miles, 2),
            'total_revenue': round(total_revenue, 2),
            'total_costs': round(total_costs, 2),
            'average_efficiency': round(avg_efficiency, 2),
            'average_speed': round(avg_speed, 2),
            'on_time_percentage': round(on_time_percentage, 2),
            'revenue_per_mile': round(total_revenue / total_miles, 2) if total_miles else 0,
            'cost_per_mile': round(total_costs / total_miles, 2) if total_miles else 0,
            'profit_margin': self.calculate_profit_margin(total_revenue, total_costs)
        }
    
    def calculate_vehicle_utilization(self, routes: list, 
                                    vehicle_capacity: float) -> Dict[str, Any]:
        """
        Calculate utilization metrics for a vehicle.
        """
        if not routes:
            return {}
        
        total_miles = sum(route.get('total_miles', 0) or 0 for route in routes)
        total_capacity_used = sum(
            route.get('load_weight', 0) or 0 for route in routes
        )
        
        # Calculate utilization percentage
        max_capacity = vehicle_capacity * len(routes)
        utilization_percentage = (total_capacity_used / max_capacity * 100) if max_capacity else 0
        
        # Fuel efficiency
        total_fuel = sum(route.get('fuel_consumed', 0) or 0 for route in routes)
        avg_mpg = (total_miles / total_fuel) if total_fuel else 0
        
        return {
            'total_routes': len(routes),
            'total_miles': round(total_miles, 2),
            'utilization_percentage': round(utilization_percentage, 2),
            'average_mpg': round(avg_mpg, 2),
            'total_fuel_consumed': round(total_fuel, 2)
        }