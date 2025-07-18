import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import Route, Driver, Vehicle, Customer, Location, RouteStatus, LoadType
from database.operations import DatabaseOperations
from data_processing.cleaner import DataCleaner
from data_processing.calculator import RouteCalculator
from utils.logger import get_logger

logger = get_logger(__name__)

class DataTransformer:
    """
    Transform raw route data into structured database records.
    Handles data mapping, validation, and normalization.
    """
    
    def __init__(self, db_operations: DatabaseOperations):
        self.db_ops = db_operations
        self.cleaner = DataCleaner()
        self.calculator = RouteCalculator()
        
    def transform_raw_data(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
        """
        Transform raw data into structured database records.
        Returns a dictionary with lists of model instances.
        """
        logger.info(f"Transforming {len(raw_data)} raw records")
        
        # Clean the data first
        cleaned_data = []
        for record in raw_data:
            cleaned_record = self.cleaner.clean_route_data(record)
            if self._validate_record(cleaned_record):
                cleaned_data.append(cleaned_record)
        
        logger.info(f"Cleaned data: {len(cleaned_data)} valid records")
        
        # Extract and create entities
        locations = self._extract_locations(cleaned_data)
        customers = self._extract_customers(cleaned_data)
        drivers = self._extract_drivers(cleaned_data)
        vehicles = self._extract_vehicles(cleaned_data)
        routes = self._extract_routes(cleaned_data)
        
        return {
            'locations': locations,
            'customers': customers,
            'drivers': drivers,
            'vehicles': vehicles,
            'routes': routes
        }
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """
        Validate that a record has minimum required fields.
        """
        required_fields = ['route_date']
        
        for field in required_fields:
            if not record.get(field):
                logger.warning(f"Record missing required field: {field}")
                return False
        
        return True
    
    def _extract_locations(self, data: List[Dict[str, Any]]) -> List[Location]:
        """
        Extract and deduplicate location data.
        """
        locations = []
        seen_locations = set()
        
        for record in data:
            for location_type in ['start_location', 'end_location']:
                if location_type in record and record[location_type]:
                    loc_data = record[location_type]
                    
                    # Create a unique key for deduplication
                    key = (
                        loc_data.get('address', '').lower(),
                        loc_data.get('city', '').lower(),
                        loc_data.get('state', '').lower(),
                        loc_data.get('zip_code', '')
                    )
                    
                    if key not in seen_locations and any(key):
                        seen_locations.add(key)
                        
                        location = Location(
                            address=loc_data.get('address', ''),
                            city=loc_data.get('city', ''),
                            state=loc_data.get('state', ''),
                            zip_code=loc_data.get('zip_code', ''),
                            latitude=loc_data.get('latitude'),
                            longitude=loc_data.get('longitude')
                        )
                        locations.append(location)
        
        logger.info(f"Extracted {len(locations)} unique locations")
        return locations
    
    def _extract_customers(self, data: List[Dict[str, Any]]) -> List[Customer]:
        """
        Extract and deduplicate customer data.
        """
        customers = []
        seen_customers = set()
        
        for record in data:
            if 'customer' in record and record['customer']:
                cust_data = record['customer']
                
                # Create a unique key for deduplication
                key = (
                    cust_data.get('name', '').lower(),
                    cust_data.get('phone', ''),
                    cust_data.get('email', '')
                )
                
                if key not in seen_customers and cust_data.get('name'):
                    seen_customers.add(key)
                    
                    customer = Customer(
                        name=cust_data.get('name', ''),
                        contact_person=cust_data.get('contact_person', ''),
                        phone=cust_data.get('phone', ''),
                        email=cust_data.get('email', ''),
                        address=cust_data.get('address', ''),
                        city=cust_data.get('city', ''),
                        state=cust_data.get('state', ''),
                        zip_code=cust_data.get('zip_code', ''),
                        payment_terms=cust_data.get('payment_terms', ''),
                        rating=cust_data.get('rating'),
                        notes=cust_data.get('notes', '')
                    )
                    customers.append(customer)
        
        logger.info(f"Extracted {len(customers)} unique customers")
        return customers
    
    def _extract_drivers(self, data: List[Dict[str, Any]]) -> List[Driver]:
        """
        Extract and deduplicate driver data.
        """
        drivers = []
        seen_drivers = set()
        
        for record in data:
            if 'driver' in record and record['driver']:
                driver_data = record['driver']
                
                # Create a unique key for deduplication
                key = (
                    driver_data.get('name', '').lower(),
                    driver_data.get('license_number', ''),
                    driver_data.get('phone', '')
                )
                
                if key not in seen_drivers and driver_data.get('name'):
                    seen_drivers.add(key)
                    
                    driver = Driver(
                        name=driver_data.get('name', ''),
                        license_number=driver_data.get('license_number', ''),
                        phone=driver_data.get('phone', ''),
                        email=driver_data.get('email', ''),
                        hire_date=driver_data.get('hire_date'),
                        hourly_rate=driver_data.get('hourly_rate'),
                        performance_rating=driver_data.get('performance_rating'),
                        safety_score=driver_data.get('safety_score'),
                        active=driver_data.get('active', True)
                    )
                    drivers.append(driver)
        
        logger.info(f"Extracted {len(drivers)} unique drivers")
        return drivers
    
    def _extract_vehicles(self, data: List[Dict[str, Any]]) -> List[Vehicle]:
        """
        Extract and deduplicate vehicle data.
        """
        vehicles = []
        seen_vehicles = set()
        
        for record in data:
            if 'vehicle' in record and record['vehicle']:
                vehicle_data = record['vehicle']
                
                # Create a unique key for deduplication
                key = (
                    vehicle_data.get('license_plate', '').lower(),
                    vehicle_data.get('vin', '').lower()
                )
                
                if key not in seen_vehicles and (vehicle_data.get('license_plate') or vehicle_data.get('vin')):
                    seen_vehicles.add(key)
                    
                    vehicle = Vehicle(
                        make=vehicle_data.get('make', ''),
                        model=vehicle_data.get('model', ''),
                        year=vehicle_data.get('year', 0),
                        license_plate=vehicle_data.get('license_plate', ''),
                        vin=vehicle_data.get('vin', ''),
                        capacity_weight=vehicle_data.get('capacity_weight'),
                        capacity_volume=vehicle_data.get('capacity_volume'),
                        fuel_type=vehicle_data.get('fuel_type', ''),
                        mpg_average=vehicle_data.get('mpg_average'),
                        last_maintenance=vehicle_data.get('last_maintenance'),
                        next_maintenance=vehicle_data.get('next_maintenance'),
                        active=vehicle_data.get('active', True)
                    )
                    vehicles.append(vehicle)
        
        logger.info(f"Extracted {len(vehicles)} unique vehicles")
        return vehicles
    
    def _extract_routes(self, data: List[Dict[str, Any]]) -> List[Route]:
        """
        Extract route data and link to other entities.
        """
        routes = []
        
        for record in data:
            # Parse load type
            load_type = LoadType.GENERAL
            if record.get('load_type'):
                try:
                    load_type = LoadType(record['load_type'].lower())
                except ValueError:
                    logger.warning(f"Unknown load type: {record['load_type']}")
            
            # Parse status
            status = RouteStatus.SCHEDULED
            if record.get('status'):
                try:
                    status = RouteStatus(record['status'].lower())
                except ValueError:
                    logger.warning(f"Unknown status: {record['status']}")
            
            route = Route(
                route_date=record.get('route_date'),
                load_weight=record.get('load_weight'),
                load_type=load_type,
                load_value=record.get('load_value'),
                special_requirements=record.get('special_requirements', ''),
                scheduled_start_time=record.get('scheduled_start_time'),
                actual_start_time=record.get('actual_start_time'),
                scheduled_end_time=record.get('scheduled_end_time'),
                actual_end_time=record.get('actual_end_time'),
                total_miles=record.get('total_miles'),
                empty_miles=record.get('empty_miles'),
                fuel_consumed=record.get('fuel_consumed'),
                average_speed=record.get('average_speed'),
                revenue=record.get('revenue'),
                fuel_cost=record.get('fuel_cost'),
                toll_cost=record.get('toll_cost'),
                driver_pay=record.get('driver_pay'),
                other_costs=record.get('other_costs'),
                status=status,
                notes=record.get('notes', '')
            )
            routes.append(route)
        
        logger.info(f"Extracted {len(routes)} routes")
        return routes
    
    def enrich_route_data(self, routes: List[Route]) -> List[Route]:
        """
        Enrich route data with calculated metrics.
        """
        logger.info(f"Enriching {len(routes)} routes with calculated metrics")
        
        enriched_routes = []
        
        for route in routes:
            # Convert route to dictionary for calculations
            route_dict = {
                'total_miles': route.total_miles,
                'empty_miles': route.empty_miles,
                'fuel_consumed': route.fuel_consumed,
                'revenue': route.revenue,
                'actual_start_time': route.actual_start_time,
                'actual_end_time': route.actual_end_time,
                'scheduled_start_time': route.scheduled_start_time,
                'scheduled_end_time': route.scheduled_end_time
            }
            
            # Calculate metrics
            metrics = self.calculator.calculate_route_metrics(route_dict)
            
            # Update route with calculated values
            if not route.average_speed and metrics.get('average_speed'):
                route.average_speed = metrics['average_speed']
            
            if not route.fuel_cost and metrics.get('fuel_cost'):
                route.fuel_cost = metrics['fuel_cost']
            
            if not route.toll_cost and metrics.get('toll_cost'):
                route.toll_cost = metrics['toll_cost']
            
            if not route.driver_pay and metrics.get('driver_pay'):
                route.driver_pay = metrics['driver_pay']
            
            enriched_routes.append(route)
        
        return enriched_routes
    
    def transform_dataframe(self, df: pd.DataFrame) -> Dict[str, List[Any]]:
        """
        Transform a pandas DataFrame into structured records.
        """
        logger.info(f"Transforming DataFrame with {len(df)} records")
        
        # Clean the DataFrame
        cleaned_df = self.cleaner.clean_dataframe(df)
        
        # Convert to list of dictionaries
        records = cleaned_df.to_dict('records')
        
        # Transform using existing method
        return self.transform_raw_data(records)
    
    def map_legacy_data(self, legacy_data: List[Dict[str, Any]], 
                       mapping_config: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Map legacy data field names to current schema.
        """
        logger.info(f"Mapping legacy data with {len(legacy_data)} records")
        
        mapped_data = []
        
        for record in legacy_data:
            mapped_record = {}
            
            # Apply field mapping
            for current_field, legacy_field in mapping_config.items():
                if legacy_field in record:
                    mapped_record[current_field] = record[legacy_field]
            
            # Handle nested structures
            if 'start_location' in mapped_record:
                mapped_record['start_location'] = self._map_location_fields(
                    mapped_record['start_location'], mapping_config
                )
            
            if 'end_location' in mapped_record:
                mapped_record['end_location'] = self._map_location_fields(
                    mapped_record['end_location'], mapping_config
                )
            
            mapped_data.append(mapped_record)
        
        return mapped_data
    
    def _map_location_fields(self, location_data: Dict[str, Any], 
                           mapping_config: Dict[str, str]) -> Dict[str, Any]:
        """
        Map location-specific fields.
        """
        mapped_location = {}
        
        location_mappings = {
            'address': ['street', 'street_address', 'addr'],
            'city': ['city', 'municipality'],
            'state': ['state', 'province', 'region'],
            'zip_code': ['zip', 'postal_code', 'zipcode'],
            'latitude': ['lat', 'latitude'],
            'longitude': ['lon', 'longitude', 'lng']
        }
        
        for current_field, possible_fields in location_mappings.items():
            for possible_field in possible_fields:
                if possible_field in location_data:
                    mapped_location[current_field] = location_data[possible_field]
                    break
        
        return mapped_location
    
    def normalize_data_types(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize data types to match database schema.
        """
        logger.info(f"Normalizing data types for {len(data)} records")
        
        normalized_data = []
        
        for record in data:
            normalized_record = {}
            
            # Normalize each field based on expected type
            for field, value in record.items():
                normalized_record[field] = self._normalize_field_value(field, value)
            
            normalized_data.append(normalized_record)
        
        return normalized_data
    
    def _normalize_field_value(self, field: str, value: Any) -> Any:
        """
        Normalize a single field value based on its expected type.
        """
        if pd.isna(value):
            return None
        
        # Datetime fields
        if any(keyword in field.lower() for keyword in ['date', 'time']):
            return self.cleaner.clean_datetime(value)
        
        # Numeric fields
        if any(keyword in field.lower() for keyword in [
            'miles', 'weight', 'value', 'cost', 'pay', 'rate', 'speed',
            'consumed', 'revenue', 'latitude', 'longitude'
        ]):
            return self.cleaner.clean_numeric(value)
        
        # String fields
        if isinstance(value, str):
            return value.strip()
        
        return value
    
    def validate_transformed_data(self, transformed_data: Dict[str, List[Any]]) -> bool:
        """
        Validate transformed data before database insertion.
        """
        logger.info("Validating transformed data")
        
        validation_errors = []
        
        # Check for required entities
        if not transformed_data.get('routes'):
            validation_errors.append("No routes found in transformed data")
        
        # Validate routes
        for i, route in enumerate(transformed_data.get('routes', [])):
            if not route.route_date:
                validation_errors.append(f"Route {i}: Missing route_date")
        
        # Check for data consistency
        route_count = len(transformed_data.get('routes', []))
        location_count = len(transformed_data.get('locations', []))
        
        if route_count > 0 and location_count == 0:
            validation_errors.append("Routes found but no locations extracted")
        
        if validation_errors:
            logger.error(f"Validation errors: {validation_errors}")
            return False
        
        logger.info("Data validation passed")
        return True
    
    def get_transformation_summary(self, transformed_data: Dict[str, List[Any]]) -> Dict[str, int]:
        """
        Get summary statistics of transformed data.
        """
        return {
            'locations': len(transformed_data.get('locations', [])),
            'customers': len(transformed_data.get('customers', [])),
            'drivers': len(transformed_data.get('drivers', [])),
            'vehicles': len(transformed_data.get('vehicles', [])),
            'routes': len(transformed_data.get('routes', []))
        }