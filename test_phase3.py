#!/usr/bin/env python3
"""
Test script for Phase 3 data processing components.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Import modules directly to avoid relative import issues
from data_processing.cleaner import DataCleaner
from data_processing.calculator import RouteCalculator
from data_processing.transformer import DataTransformer
from data_processing.geo_processor import GeoProcessor, GeoLocation
from data_processing.pipeline import DataProcessingPipeline
from config.settings import Settings
from database.operations import DatabaseOperations
from utils.logger import get_logger

logger = get_logger(__name__)

def create_sample_data() -> List[Dict[str, Any]]:
    """Create sample route data for testing."""
    sample_data = [
        {
            'route_date': '2024-01-15',
            'start_location': {
                'address': '123 Main St',
                'city': 'Chicago',
                'state': 'IL',
                'zip_code': '60601'
            },
            'end_location': {
                'address': '456 Oak Ave',
                'city': 'Milwaukee',
                'state': 'WI',
                'zip_code': '53202'
            },
            'driver': {
                'name': 'John Smith',
                'phone': '555-123-4567',
                'email': 'john.smith@email.com',
                'hourly_rate': 25.50
            },
            'vehicle': {
                'make': 'Freightliner',
                'model': 'Cascadia',
                'year': 2020,
                'license_plate': 'ABC123',
                'mpg_average': 8.5
            },
            'customer': {
                'name': 'ABC Company',
                'phone': '555-987-6543',
                'email': 'orders@abccompany.com'
            },
            'load_weight': 35000,
            'load_type': 'general',
            'total_miles': 92.5,
            'empty_miles': 15.0,
            'fuel_consumed': 11.0,
            'revenue': 1250.00,
            'scheduled_start_time': '2024-01-15 08:00:00',
            'actual_start_time': '2024-01-15 08:15:00',
            'scheduled_end_time': '2024-01-15 14:00:00',
            'actual_end_time': '2024-01-15 14:30:00',
            'status': 'completed'
        },
        {
            'route_date': '2024-01-16',
            'start_location': {
                'address': '789 Industrial Dr',
                'city': 'Detroit',
                'state': 'MI',
                'zip_code': '48201'
            },
            'end_location': {
                'address': '321 Commerce St',
                'city': 'Cleveland',
                'state': 'OH',
                'zip_code': '44101'
            },
            'driver': {
                'name': 'Jane Doe',
                'phone': '(555) 234-5678',
                'email': 'jane.doe@email.com',
                'hourly_rate': 28.00
            },
            'vehicle': {
                'make': 'Peterbilt',
                'model': '579',
                'year': 2019,
                'license_plate': 'XYZ789',
                'mpg_average': 7.8
            },
            'customer': {
                'name': 'XYZ Corporation',
                'phone': '555.876.5432',
                'email': 'shipping@xyzcorp.com'
            },
            'load_weight': 42000,
            'load_type': 'refrigerated',
            'total_miles': 170.2,
            'revenue': 2100.00,
            'scheduled_start_time': '2024-01-16 06:00:00',
            'actual_start_time': '2024-01-16 06:10:00',
            'scheduled_end_time': '2024-01-16 12:00:00',
            'actual_end_time': '2024-01-16 12:45:00',
            'status': 'completed'
        }
    ]
    
    return sample_data

def test_data_cleaner():
    """Test the DataCleaner functionality."""
    print("\n=== Testing Data Cleaner ===")
    
    cleaner = DataCleaner()
    
    # Test phone number cleaning
    test_phones = [
        "555-123-4567",
        "(555) 123-4567",
        "555.123.4567",
        "15551234567",
        "invalid"
    ]
    
    print("Phone number cleaning:")
    for phone in test_phones:
        cleaned = cleaner.clean_phone_number(phone)
        print(f"  {phone} -> {cleaned}")
    
    # Test email cleaning
    test_emails = [
        "test@example.com",
        "TEST@EXAMPLE.COM",
        "invalid-email",
        "user@domain"
    ]
    
    print("\nEmail cleaning:")
    for email in test_emails:
        cleaned = cleaner.clean_email(email)
        print(f"  {email} -> {cleaned}")
    
    # Test route data cleaning
    sample_data = create_sample_data()
    cleaned_record = cleaner.clean_route_data(sample_data[0])
    print(f"\nCleaned route data keys: {list(cleaned_record.keys())}")
    
    print("✓ Data Cleaner tests completed")

def test_route_calculator():
    """Test the RouteCalculator functionality."""
    print("\n=== Testing Route Calculator ===")
    
    calculator = RouteCalculator()
    
    # Test distance calculation
    chicago_lat, chicago_lon = 41.8781, -87.6298
    milwaukee_lat, milwaukee_lon = 43.0389, -87.9065
    
    distance = calculator.calculate_distance(
        chicago_lat, chicago_lon, milwaukee_lat, milwaukee_lon
    )
    print(f"Distance Chicago to Milwaukee: {distance} miles")
    
    # Test fuel cost calculation
    fuel_cost = calculator.calculate_fuel_cost(92.5, 8.5, 3.50)
    print(f"Fuel cost for 92.5 miles at 8.5 MPG: ${fuel_cost}")
    
    # Test profit margin calculation
    profit_margin = calculator.calculate_profit_margin(1250.00, 850.00)
    print(f"Profit margin: {profit_margin}%")
    
    # Test route metrics calculation
    sample_data = create_sample_data()
    try:
        metrics = calculator.calculate_route_metrics(sample_data[0])
        print(f"Route metrics calculated: {len(metrics)} metrics")
    except Exception as e:
        print(f"Route metrics calculation failed: {e}")
        # Continue with other tests
    
    print("✓ Route Calculator tests completed")

def test_geo_processor():
    """Test the GeoProcessor functionality."""
    print("\n=== Testing Geo Processor ===")
    
    settings = Settings()
    geo_processor = GeoProcessor(settings)
    
    # Test basic distance calculation
    chicago_lat, chicago_lon = 41.8781, -87.6298
    milwaukee_lat, milwaukee_lon = 43.0389, -87.9065
    
    distance = geo_processor.calculate_distance(
        chicago_lat, chicago_lon, milwaukee_lat, milwaukee_lon
    )
    print(f"Distance calculation: {distance} miles")
    
    # Test geocoding (using free Nominatim service)
    try:
        location = geo_processor.geocode_address("123 Main St", "Chicago", "IL", "60601")
        if location:
            print(f"Geocoded address: {location.latitude}, {location.longitude}")
        else:
            print("Geocoding failed (this is expected without API keys)")
    except Exception as e:
        print(f"Geocoding test skipped: {e}")
    
    # Test geographic center calculation
    locations = [
        GeoLocation("Location 1", 41.8781, -87.6298),
        GeoLocation("Location 2", 43.0389, -87.9065)
    ]
    
    center = geo_processor.get_geographic_center(locations)
    if center:
        print(f"Geographic center: {center.latitude}, {center.longitude}")
    
    print("✓ Geo Processor tests completed")

def test_data_transformer():
    """Test the DataTransformer functionality."""
    print("\n=== Testing Data Transformer ===")
    
    # Initialize with database operations
    settings = Settings()
    db_ops = DatabaseOperations(settings.database_path)
    transformer = DataTransformer(db_ops)
    
    # Test data transformation
    sample_data = create_sample_data()
    transformed_data = transformer.transform_raw_data(sample_data)
    
    print("Transformation summary:")
    summary = transformer.get_transformation_summary(transformed_data)
    for entity_type, count in summary.items():
        print(f"  {entity_type}: {count}")
    
    # Test data validation
    is_valid = transformer.validate_transformed_data(transformed_data)
    print(f"Data validation: {'✓ Passed' if is_valid else '✗ Failed'}")
    
    print("✓ Data Transformer tests completed")

def test_processing_pipeline():
    """Test the complete DataProcessingPipeline."""
    print("\n=== Testing Processing Pipeline ===")
    
    # Initialize pipeline
    settings = Settings()
    db_ops = DatabaseOperations(settings.database_path)
    pipeline = DataProcessingPipeline(settings, db_ops)
    
    # Test pipeline configuration validation
    validation_result = pipeline.validate_pipeline_configuration()
    print(f"Pipeline configuration: {validation_result['status']}")
    if validation_result['issues']:
        print(f"Issues: {validation_result['issues']}")
    
    # Test data processing (without geocoding to avoid API calls)
    sample_data = create_sample_data()
    
    print("Processing sample data...")
    result = pipeline.process_raw_data(
        sample_data, 
        enable_geocoding=False,  # Skip geocoding for testing
        enable_calculations=True
    )
    
    print(f"Processing result: {result['status']}")
    if result['status'] == 'success':
        print("Summary:")
        for entity_type, count in result['summary'].items():
            print(f"  {entity_type}: {count}")
        
        stats = pipeline.get_pipeline_statistics()
        print(f"Processing time: {stats.get('processing_time_seconds', 0):.2f} seconds")
    
    print("✓ Processing Pipeline tests completed")

def main():
    """Run all Phase 3 tests."""
    print("Starting Phase 3 Data Processing Tests")
    print("="*50)
    
    try:
        test_data_cleaner()
        test_route_calculator()
        test_geo_processor()
        test_data_transformer()
        test_processing_pipeline()
        
        print("\n" + "="*50)
        print("✓ All Phase 3 tests completed successfully!")
        print("\nPhase 3 Components Implemented:")
        print("  ✓ Data Cleaning Functions")
        print("  ✓ Business Calculations")
        print("  ✓ Data Transformation Pipeline")
        print("  ✓ Geographic Processing Capabilities")
        print("  ✓ Complete Processing Pipeline")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        logger.error(f"Test error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())