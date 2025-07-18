import json
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import tempfile

from .base_collector import BaseCollector, CollectionResult, CollectionStatus
from utils.helpers import safe_float_convert, safe_int_convert, parse_date, clean_string, ensure_directory_exists

class ManualEntryCollector(BaseCollector):
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        
        # Manual entry configuration
        self.entry_file_path = config.get('entry_file_path', 'data/input/manual_entry.json')
        self.template_file_path = config.get('template_file_path', 'data/input/manual_entry_template.json')
        self.processed_directory = config.get('processed_directory', 'data/archive/manual_entry')
        self.auto_create_template = config.get('auto_create_template', True)
        
        # Entry validation
        self.require_all_fields = config.get('require_all_fields', False)
        self.allowed_batch_size = config.get('allowed_batch_size', 100)
        
        self._ensure_directories()
        
        if self.auto_create_template:
            self._create_template_if_needed()
    
    def _ensure_directories(self):
        """Ensure all required directories exist"""
        ensure_directory_exists(os.path.dirname(self.entry_file_path))
        ensure_directory_exists(os.path.dirname(self.template_file_path))
        ensure_directory_exists(self.processed_directory)
    
    def validate_configuration(self) -> bool:
        """Validate manual entry collector configuration"""
        if not self.entry_file_path:
            self.logger.error("Entry file path is required")
            return False
        
        # Check if we can write to the directories
        try:
            test_file = os.path.join(os.path.dirname(self.entry_file_path), '.test_write')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
        except Exception as e:
            self.logger.error(f"Cannot write to entry directory: {str(e)}")
            return False
        
        return True
    
    def test_connection(self) -> bool:
        """Test connection (file system access)"""
        return self.validate_configuration()
    
    def collect_data(self) -> CollectionResult:
        """Collect data from manual entry file"""
        if not self.validate_configuration():
            return CollectionResult(
                status=CollectionStatus.FAILED,
                data=[],
                errors=["Invalid configuration"],
                warnings=[],
                metadata={},
                collected_at=datetime.now(),
                source_info=self.get_source_info()
            )
        
        # Check if entry file exists
        if not os.path.exists(self.entry_file_path):
            return CollectionResult(
                status=CollectionStatus.NO_DATA,
                data=[],
                errors=[],
                warnings=[f"Manual entry file not found: {self.entry_file_path}"],
                metadata={'file_path': self.entry_file_path},
                collected_at=datetime.now(),
                source_info=self.get_source_info()
            )
        
        try:
            # Load data from file
            with open(self.entry_file_path, 'r', encoding='utf-8') as f:
                entry_data = json.load(f)
            
            # Process the data
            processed_result = self._process_manual_entry_data(entry_data)
            
            if processed_result['success']:
                # Move processed file to archive
                self._archive_processed_file()
            
            return self.process_collected_data(processed_result['data'])
            
        except json.JSONDecodeError as e:
            return CollectionResult(
                status=CollectionStatus.FAILED,
                data=[],
                errors=[f"Invalid JSON format in manual entry file: {str(e)}"],
                warnings=[],
                metadata={'file_path': self.entry_file_path},
                collected_at=datetime.now(),
                source_info=self.get_source_info()
            )
        except Exception as e:
            return CollectionResult(
                status=CollectionStatus.FAILED,
                data=[],
                errors=[f"Error reading manual entry file: {str(e)}"],
                warnings=[],
                metadata={'file_path': self.entry_file_path},
                collected_at=datetime.now(),
                source_info=self.get_source_info()
            )
    
    def _process_manual_entry_data(self, entry_data: Any) -> Dict[str, Any]:
        """Process manual entry data"""
        processed_data = []
        errors = []
        warnings = []
        
        # Handle different data formats
        if isinstance(entry_data, dict):
            if 'routes' in entry_data:
                # Structured format with routes array
                routes_data = entry_data['routes']
                metadata = entry_data.get('metadata', {})
            elif 'data' in entry_data:
                # Generic format with data array
                routes_data = entry_data['data']
                metadata = entry_data.get('metadata', {})
            else:
                # Single route object
                routes_data = [entry_data]
                metadata = {}
        elif isinstance(entry_data, list):
            # Array of routes
            routes_data = entry_data
            metadata = {}
        else:
            return {
                'success': False,
                'data': [],
                'errors': ["Invalid data format in manual entry file"],
                'warnings': []
            }
        
        # Validate batch size
        if len(routes_data) > self.allowed_batch_size:
            warnings.append(f"Batch size ({len(routes_data)}) exceeds limit ({self.allowed_batch_size})")
        
        # Process each route
        for i, route_data in enumerate(routes_data):
            try:
                # Validate route data
                validation_errors = self._validate_manual_entry_record(route_data, i)
                if validation_errors:
                    errors.extend(validation_errors)
                    continue
                
                # Clean and process the record
                cleaned_record = self._clean_manual_entry_record(route_data)
                processed_data.append(cleaned_record)
                
            except Exception as e:
                error_msg = f"Error processing manual entry record {i}: {str(e)}"
                errors.append(error_msg)
                self.logger.warning(error_msg)
        
        return {
            'success': len(errors) == 0,
            'data': processed_data,
            'errors': errors,
            'warnings': warnings,
            'metadata': metadata
        }
    
    def _validate_manual_entry_record(self, record: Dict[str, Any], index: int) -> List[str]:
        """Validate a manual entry record"""
        errors = []
        
        if not isinstance(record, dict):
            errors.append(f"Record {index}: Expected object, got {type(record)}")
            return errors
        
        # Check required fields if configured
        if self.require_all_fields:
            required_fields = self.get_required_fields()
            for field in required_fields:
                if field not in record or record[field] is None or record[field] == "":
                    errors.append(f"Record {index}: Missing required field '{field}'")
        
        # Validate specific field types and formats
        validations = [
            ('route_id', str, "Route ID must be a string"),
            ('route_date', None, "Route date is required"),
            ('total_miles', (int, float, str), "Total miles must be numeric"),
            ('revenue', (int, float, str), "Revenue must be numeric"),
            ('load_weight', (int, float, str), "Load weight must be numeric")
        ]
        
        for field_name, expected_types, error_message in validations:
            if field_name in record and record[field_name] is not None:
                value = record[field_name]
                
                if field_name == 'route_date':
                    # Special handling for dates
                    if not parse_date(str(value)):
                        errors.append(f"Record {index}: Invalid date format for {field_name}")
                elif expected_types and not isinstance(value, expected_types):
                    # For numeric fields, try to convert
                    if field_name in ['total_miles', 'revenue', 'load_weight']:
                        if safe_float_convert(value) is None:
                            errors.append(f"Record {index}: {error_message}")
                    else:
                        errors.append(f"Record {index}: {error_message}")
        
        return errors
    
    def _clean_manual_entry_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize a manual entry record"""
        cleaned = {}
        
        # Add manual entry metadata
        cleaned['source'] = 'manual_entry'
        cleaned['entry_timestamp'] = datetime.now().isoformat()
        
        for key, value in record.items():
            if value is None:
                continue
            
            # Clean key names
            clean_key = key.strip().lower().replace(' ', '_').replace('-', '_')
            
            # Clean and convert values based on field type
            if clean_key in ['total_miles', 'revenue', 'load_weight', 'fuel_cost', 'toll_cost', 'driver_pay']:
                cleaned[clean_key] = safe_float_convert(value)
            elif clean_key in ['route_date', 'start_time', 'end_time', 'scheduled_start_time', 'scheduled_end_time']:
                parsed_date = parse_date(str(value))
                cleaned[clean_key] = parsed_date.isoformat() if parsed_date else None
            elif isinstance(value, str):
                cleaned[clean_key] = clean_string(value)
            else:
                cleaned[clean_key] = value
        
        return cleaned
    
    def _archive_processed_file(self):
        """Move processed manual entry file to archive"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = os.path.basename(self.entry_file_path)
            name, ext = os.path.splitext(filename)
            
            archive_filename = f"{name}_{timestamp}{ext}"
            archive_path = os.path.join(self.processed_directory, archive_filename)
            
            # Move file to archive
            import shutil
            shutil.move(self.entry_file_path, archive_path)
            
            self.logger.info(f"Archived manual entry file to: {archive_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to archive manual entry file: {str(e)}")
    
    def _create_template_if_needed(self):
        """Create template file if it doesn't exist"""
        if os.path.exists(self.template_file_path):
            return
        
        try:
            template_data = {
                "metadata": {
                    "description": "Manual entry template for route data",
                    "created_at": datetime.now().isoformat(),
                    "instructions": [
                        "1. Fill in the route data in the 'routes' array below",
                        "2. Save this file as the configured entry file name",
                        "3. The system will automatically process and archive the file",
                        "4. Required fields: route_id, route_date",
                        "5. Optional fields: driver_name, vehicle_id, customer_name, etc."
                    ]
                },
                "routes": [
                    {
                        "route_id": "EXAMPLE001",
                        "route_date": "2024-01-15",
                        "driver_name": "John Doe",
                        "vehicle_id": "TRUCK001",
                        "customer_name": "ABC Company",
                        "origin_address": "123 Pickup St, City, ST 12345",
                        "destination_address": "456 Delivery Ave, Other City, ST 67890",
                        "total_miles": 250.5,
                        "revenue": 1500.00,
                        "load_weight": 15000,
                        "load_type": "general",
                        "status": "completed",
                        "start_time": "2024-01-15T08:00:00",
                        "end_time": "2024-01-15T16:30:00",
                        "fuel_cost": 125.50,
                        "toll_cost": 25.00,
                        "driver_pay": 200.00,
                        "notes": "Delivery completed on time"
                    }
                ]
            }
            
            with open(self.template_file_path, 'w', encoding='utf-8') as f:
                json.dump(template_data, f, indent=2)
            
            self.logger.info(f"Created manual entry template: {self.template_file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to create template file: {str(e)}")
    
    def create_entry_from_data(self, routes_data: List[Dict[str, Any]], metadata: Dict[str, Any] = None) -> str:
        """Create a manual entry file from provided data"""
        try:
            entry_data = {
                "metadata": metadata or {
                    "created_at": datetime.now().isoformat(),
                    "created_by": "system"
                },
                "routes": routes_data
            }
            
            # Generate unique filename if original exists
            entry_path = self.entry_file_path
            if os.path.exists(entry_path):
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                name, ext = os.path.splitext(entry_path)
                entry_path = f"{name}_{timestamp}{ext}"
            
            with open(entry_path, 'w', encoding='utf-8') as f:
                json.dump(entry_data, f, indent=2)
            
            self.logger.info(f"Created manual entry file: {entry_path}")
            return entry_path
            
        except Exception as e:
            self.logger.error(f"Failed to create manual entry file: {str(e)}")
            raise
    
    def get_template_data(self) -> Dict[str, Any]:
        """Get template data for manual entry"""
        return {
            "route_id": "",
            "route_date": datetime.now().strftime('%Y-%m-%d'),
            "driver_name": "",
            "vehicle_id": "",
            "customer_name": "",
            "origin_address": "",
            "destination_address": "",
            "total_miles": 0.0,
            "revenue": 0.0,
            "load_weight": 0.0,
            "load_type": "general",
            "status": "scheduled",
            "start_time": "",
            "end_time": "",
            "fuel_cost": 0.0,
            "toll_cost": 0.0,
            "driver_pay": 0.0,
            "notes": ""
        }
    
    def get_required_fields(self) -> List[str]:
        """Get required fields for manual entry"""
        return ['route_id', 'route_date']
    
    def standardize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert manual entry record to standard route data format"""
        standardized = {
            'source': 'manual_entry',
            'source_name': self.name,
            'collected_at': datetime.now().isoformat(),
            'raw_data': record.copy()
        }
        
        # Copy entry timestamp if available
        if 'entry_timestamp' in record:
            standardized['entry_timestamp'] = record['entry_timestamp']
        
        # Direct mapping for manual entry (fields should already be in standard format)
        standard_fields = [
            'route_id', 'route_date', 'driver_name', 'vehicle_id', 'customer_name',
            'origin_address', 'destination_address', 'total_miles', 'revenue',
            'load_weight', 'load_type', 'status', 'start_time', 'end_time',
            'fuel_cost', 'toll_cost', 'driver_pay', 'notes'
        ]
        
        for field in standard_fields:
            if field in record and record[field] is not None:
                value = record[field]
                
                # Apply type conversions
                if field in ['total_miles', 'revenue', 'load_weight', 'fuel_cost', 'toll_cost', 'driver_pay']:
                    standardized[field] = safe_float_convert(value)
                elif field in ['route_date', 'start_time', 'end_time']:
                    if isinstance(value, str) and value:
                        parsed_date = parse_date(value)
                        standardized[field] = parsed_date.isoformat() if parsed_date else value
                    else:
                        standardized[field] = value
                elif isinstance(value, str):
                    standardized[field] = clean_string(value)
                else:
                    standardized[field] = value
        
        return standardized