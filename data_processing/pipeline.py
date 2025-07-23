"""
Main data processing pipeline that orchestrates all processing steps.
"""

import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_processing.cleaner import DataCleaner
from data_processing.calculator import RouteCalculator
from data_processing.transformer import DataTransformer
from data_processing.geo_processor import GeoProcessor, GeoLocation
from database.operations import DatabaseOperations
from config.settings import Settings
from utils.logger import get_logger

logger = get_logger(__name__)

class DataProcessingPipeline:
    """
    Main data processing pipeline that orchestrates all processing steps.
    """
    
    def __init__(self, settings: Settings, db_operations: DatabaseOperations):
        self.settings = settings
        self.db_ops = db_operations
        
        # Initialize processing components
        self.cleaner = DataCleaner()
        self.calculator = RouteCalculator()
        self.transformer = DataTransformer(db_operations)
        self.geo_processor = GeoProcessor(settings)
        
        # Processing statistics
        self.stats = {
            'processed_count': 0,
            'success_count': 0,
            'error_count': 0,
            'geocoding_count': 0,
            'calculation_count': 0,
            'start_time': None,
            'end_time': None
        }
    
    def process_raw_data(self, raw_data: List[Dict[str, Any]], 
                        enable_geocoding: bool = True,
                        enable_calculations: bool = True) -> Dict[str, Any]:
        """
        Process raw route data through the complete pipeline.
        
        Args:
            raw_data: List of raw route data records
            enable_geocoding: Whether to perform geocoding
            enable_calculations: Whether to perform business calculations
            
        Returns:
            Dictionary with processing results and statistics
        """
        self.stats['start_time'] = datetime.now()
        self.stats['processed_count'] = len(raw_data)
        
        logger.info(f"Starting data processing pipeline with {len(raw_data)} records")
        
        try:
            # Step 1: Clean and validate data
            logger.info("Step 1: Cleaning and validating data")
            cleaned_data = self._clean_data(raw_data)
            
            # Step 2: Geocode addresses if enabled
            if enable_geocoding:
                logger.info("Step 2: Geocoding addresses")
                geocoded_data = self._geocode_addresses(cleaned_data)
            else:
                geocoded_data = cleaned_data
            
            # Step 3: Calculate business metrics if enabled
            if enable_calculations:
                logger.info("Step 3: Calculating business metrics")
                calculated_data = self._calculate_metrics(geocoded_data)
            else:
                calculated_data = geocoded_data
            
            # Step 4: Transform data into database entities
            logger.info("Step 4: Transforming data into database entities")
            transformed_data = self.transformer.transform_raw_data(calculated_data)
            
            # Step 5: Validate transformed data
            logger.info("Step 5: Validating transformed data")
            if not self.transformer.validate_transformed_data(transformed_data):
                raise ValueError("Data validation failed")
            
            # Step 6: Store in database
            logger.info("Step 6: Storing data in database")
            storage_results = self._store_data(transformed_data)
            
            self.stats['success_count'] = len(calculated_data)
            self.stats['end_time'] = datetime.now()
            
            logger.info(f"Pipeline completed successfully: {self.stats['success_count']} records processed")
            
            return {
                'status': 'success',
                'processed_data': transformed_data,
                'storage_results': storage_results,
                'statistics': self.stats,
                'summary': self.transformer.get_transformation_summary(transformed_data)
            }
            
        except Exception as e:
            self.stats['error_count'] = self.stats['processed_count'] - self.stats['success_count']
            self.stats['end_time'] = datetime.now()
            
            logger.error(f"Pipeline failed: {e}")
            
            return {
                'status': 'error',
                'error': str(e),
                'statistics': self.stats
            }
    
    def _clean_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean and validate raw data."""
        cleaned_data = []
        
        for record in raw_data:
            try:
                cleaned_record = self.cleaner.clean_route_data(record)
                
                # Validate required fields
                if self.cleaner.validate_required_fields(cleaned_record, ['route_date']):
                    cleaned_data.append(cleaned_record)
                else:
                    logger.warning(f"Skipping record due to missing required fields: {record}")
                    
            except Exception as e:
                logger.error(f"Error cleaning record: {e}")
                continue
        
        # Remove duplicates
        cleaned_data = self.cleaner.remove_duplicates(cleaned_data)
        
        logger.info(f"Cleaned {len(cleaned_data)} records from {len(raw_data)} raw records")
        return cleaned_data
    
    def _geocode_addresses(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Geocode addresses in the data."""
        geocoded_data = []
        
        for record in data:
            try:
                # Geocode start location
                if 'start_location' in record and record['start_location']:
                    start_loc = record['start_location']
                    if not start_loc.get('latitude') or not start_loc.get('longitude'):
                        geo_result = self.geo_processor.geocode_address(
                            start_loc.get('address', ''),
                            start_loc.get('city', ''),
                            start_loc.get('state', ''),
                            start_loc.get('zip_code', '')
                        )
                        
                        if geo_result:
                            start_loc['latitude'] = geo_result.latitude
                            start_loc['longitude'] = geo_result.longitude
                            self.stats['geocoding_count'] += 1
                
                # Geocode end location
                if 'end_location' in record and record['end_location']:
                    end_loc = record['end_location']
                    if not end_loc.get('latitude') or not end_loc.get('longitude'):
                        geo_result = self.geo_processor.geocode_address(
                            end_loc.get('address', ''),
                            end_loc.get('city', ''),
                            end_loc.get('state', ''),
                            end_loc.get('zip_code', '')
                        )
                        
                        if geo_result:
                            end_loc['latitude'] = geo_result.latitude
                            end_loc['longitude'] = geo_result.longitude
                            self.stats['geocoding_count'] += 1
                
                geocoded_data.append(record)
                
            except Exception as e:
                logger.error(f"Error geocoding record: {e}")
                geocoded_data.append(record)  # Keep record even if geocoding fails
        
        logger.info(f"Geocoded {self.stats['geocoding_count']} addresses")
        return geocoded_data
    
    def _calculate_metrics(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Calculate business metrics for the data."""
        calculated_data = []
        
        for record in data:
            try:
                # Calculate metrics using the calculator
                metrics = self.calculator.calculate_route_metrics(record)
                
                # Merge metrics back into the record
                record.update(metrics)
                
                calculated_data.append(record)
                self.stats['calculation_count'] += 1
                
            except Exception as e:
                logger.error(f"Error calculating metrics for record: {e}")
                calculated_data.append(record)  # Keep record even if calculation fails
        
        logger.info(f"Calculated metrics for {self.stats['calculation_count']} records")
        return calculated_data
    
    def _store_data(self, transformed_data: Dict[str, List[Any]]) -> Dict[str, Any]:
        """Store transformed data in the database."""
        storage_results = {}
        
        try:
            # Store locations
            if transformed_data.get('locations'):
                location_ids = []
                for location in transformed_data['locations']:
                    location_id = self.db_ops.create_location(location)
                    location_ids.append(location_id)
                storage_results['locations'] = len(location_ids)
            
            # Store customers
            if transformed_data.get('customers'):
                customer_ids = []
                for customer in transformed_data['customers']:
                    customer_id = self.db_ops.create_customer(customer)
                    customer_ids.append(customer_id)
                storage_results['customers'] = len(customer_ids)
            
            # Store drivers
            if transformed_data.get('drivers'):
                driver_ids = []
                for driver in transformed_data['drivers']:
                    driver_id = self.db_ops.create_driver(driver)
                    driver_ids.append(driver_id)
                storage_results['drivers'] = len(driver_ids)
            
            # Store vehicles
            if transformed_data.get('vehicles'):
                vehicle_ids = []
                for vehicle in transformed_data['vehicles']:
                    vehicle_id = self.db_ops.create_vehicle(vehicle)
                    vehicle_ids.append(vehicle_id)
                storage_results['vehicles'] = len(vehicle_ids)
            
            # Store routes (with proper foreign key linking)
            if transformed_data.get('routes'):
                route_ids = []
                for route in transformed_data['routes']:
                    # Link to existing entities in database
                    route = self._link_route_entities(route)
                    route_id = self.db_ops.create_route(route)
                    route_ids.append(route_id)
                storage_results['routes'] = len(route_ids)
            
            logger.info(f"Stored data: {storage_results}")
            return storage_results
            
        except Exception as e:
            logger.error(f"Error storing data: {e}")
            raise
    
    def _link_route_entities(self, route):
        """Link route to existing database entities."""
        # This would need to be implemented based on the specific linking logic
        # For now, return the route as-is
        return route
    
    def process_dataframe(self, df: pd.DataFrame, 
                         enable_geocoding: bool = True,
                         enable_calculations: bool = True) -> Dict[str, Any]:
        """
        Process a pandas DataFrame through the pipeline.
        """
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        # Process using the main pipeline
        return self.process_raw_data(records, enable_geocoding, enable_calculations)
    
    def process_csv_file(self, file_path: str, 
                        enable_geocoding: bool = True,
                        enable_calculations: bool = True) -> Dict[str, Any]:
        """
        Process a CSV file through the pipeline.
        """
        try:
            # Load CSV file
            df = pd.read_csv(file_path)
            logger.info(f"Loaded CSV file with {len(df)} records: {file_path}")
            
            # Process the DataFrame
            return self.process_dataframe(df, enable_geocoding, enable_calculations)
            
        except Exception as e:
            logger.error(f"Error processing CSV file {file_path}: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'file_path': file_path
            }
    
    def get_pipeline_statistics(self) -> Dict[str, Any]:
        """Get current pipeline statistics."""
        stats = self.stats.copy()
        
        if stats['start_time'] and stats['end_time']:
            processing_time = stats['end_time'] - stats['start_time']
            stats['processing_time_seconds'] = processing_time.total_seconds()
            
            if stats['processed_count'] > 0:
                stats['records_per_second'] = stats['processed_count'] / processing_time.total_seconds()
        
        return stats
    
    def reset_statistics(self):
        """Reset pipeline statistics."""
        self.stats = {
            'processed_count': 0,
            'success_count': 0,
            'error_count': 0,
            'geocoding_count': 0,
            'calculation_count': 0,
            'start_time': None,
            'end_time': None
        }
    
    def validate_pipeline_configuration(self) -> Dict[str, Any]:
        """Validate that the pipeline is properly configured."""
        validation_result = {
            'status': 'valid',
            'issues': []
        }
        
        # Check database connection
        try:
            self.db_ops.get_connection()
        except Exception as e:
            validation_result['issues'].append(f"Database connection error: {e}")
        
        # Check geocoding configuration
        if not self.settings.get_google_maps_api_key() and not self.settings.get_mapbox_api_key():
            validation_result['issues'].append("No geocoding API keys configured")
        
        if validation_result['issues']:
            validation_result['status'] = 'invalid'
        
        return validation_result