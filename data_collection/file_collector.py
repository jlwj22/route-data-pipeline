import os
import pandas as pd
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import shutil
import hashlib

from .base_collector import BaseCollector, CollectionResult, CollectionStatus
from utils.helpers import (
    safe_float_convert, safe_int_convert, parse_date, clean_string,
    ensure_directory_exists, backup_file, get_file_hash
)

class FileCollector(BaseCollector):
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        
        # File collection configuration
        self.input_directory = config.get('input_directory', 'data/input')
        self.processed_directory = config.get('processed_directory', 'data/archive/processed')
        self.error_directory = config.get('error_directory', 'data/archive/errors')
        self.file_patterns = config.get('file_patterns', ['*.csv', '*.xlsx', '*.xls'])
        self.encoding = config.get('encoding', 'utf-8')
        self.delimiter = config.get('delimiter', ',')
        
        # Processing options
        self.move_processed_files = config.get('move_processed_files', True)
        self.create_backup = config.get('create_backup', True)
        self.skip_duplicates = config.get('skip_duplicates', True)
        self.max_file_age_days = config.get('max_file_age_days', 30)
        
        # Column mapping for different file formats
        self.column_mapping = config.get('column_mapping', {})
        self.required_columns = config.get('required_columns', [])
        
        # File tracking
        self.processed_files = set()
        self.file_hashes = {}
        
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure all required directories exist"""
        ensure_directory_exists(self.input_directory)
        ensure_directory_exists(self.processed_directory)
        ensure_directory_exists(self.error_directory)
    
    def validate_configuration(self) -> bool:
        """Validate file collector configuration"""
        if not os.path.exists(self.input_directory):
            self.logger.error(f"Input directory does not exist: {self.input_directory}")
            return False
        
        if not self.file_patterns:
            self.logger.error("No file patterns specified")
            return False
        
        return True
    
    def test_connection(self) -> bool:
        """Test connection to file system"""
        try:
            # Test read access to input directory
            os.listdir(self.input_directory)
            
            # Test write access to processed directory
            test_file = os.path.join(self.processed_directory, '.test_write')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            
            return True
        except Exception as e:
            self.logger.error(f"File system access test failed: {str(e)}")
            return False
    
    def collect_data(self) -> CollectionResult:
        """Collect data from files in the input directory"""
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
        
        all_data = []
        all_errors = []
        all_warnings = []
        processed_file_count = 0
        metadata = {}
        
        # Find files to process
        files_to_process = self._find_files_to_process()
        
        if not files_to_process:
            return CollectionResult(
                status=CollectionStatus.NO_DATA,
                data=[],
                errors=[],
                warnings=["No files found to process"],
                metadata={'files_checked': 0},
                collected_at=datetime.now(),
                source_info=self.get_source_info()
            )
        
        self.logger.info(f"Found {len(files_to_process)} files to process")
        
        for file_path in files_to_process:
            try:
                self.logger.info(f"Processing file: {file_path}")
                
                # Process the file
                file_result = self._process_file(file_path)
                
                if file_result['success']:
                    all_data.extend(file_result['data'])
                    processed_file_count += 1
                    
                    # Move processed file if configured
                    if self.move_processed_files:
                        self._move_processed_file(file_path)
                else:
                    all_errors.extend(file_result['errors'])
                    # Move error file to error directory
                    self._move_error_file(file_path, file_result['errors'])
                
                all_warnings.extend(file_result['warnings'])
                metadata[os.path.basename(file_path)] = file_result['metadata']
                
            except Exception as e:
                error_msg = f"Failed to process file {file_path}: {str(e)}"
                all_errors.append(error_msg)
                self.logger.error(error_msg)
                self._move_error_file(file_path, [error_msg])
        
        metadata.update({
            'total_files_found': len(files_to_process),
            'files_processed_successfully': processed_file_count,
            'files_with_errors': len(files_to_process) - processed_file_count
        })
        
        return self.process_collected_data(all_data)
    
    def _find_files_to_process(self) -> List[str]:
        """Find files in input directory that match patterns and haven't been processed"""
        files_to_process = []
        
        for pattern in self.file_patterns:
            pattern_path = Path(self.input_directory)
            matching_files = list(pattern_path.glob(pattern))
            
            for file_path in matching_files:
                file_str = str(file_path)
                
                # Skip if already processed
                if self.skip_duplicates and file_str in self.processed_files:
                    continue
                
                # Check file age
                file_age = datetime.now() - datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_age.days > self.max_file_age_days:
                    self.logger.warning(f"Skipping old file: {file_str} (age: {file_age.days} days)")
                    continue
                
                # Check for duplicate content if enabled
                if self.skip_duplicates:
                    file_hash = get_file_hash(file_str)
                    if file_hash in self.file_hashes.values():
                        self.logger.warning(f"Skipping duplicate file content: {file_str}")
                        continue
                    self.file_hashes[file_str] = file_hash
                
                files_to_process.append(file_str)
        
        return sorted(files_to_process)
    
    def _process_file(self, file_path: str) -> Dict[str, Any]:
        """Process a single file and return results"""
        file_ext = os.path.splitext(file_path)[1].lower()
        
        try:
            if file_ext == '.csv':
                data = self._read_csv_file(file_path)
            elif file_ext in ['.xlsx', '.xls']:
                data = self._read_excel_file(file_path)
            elif file_ext == '.json':
                data = self._read_json_file(file_path)
            else:
                return {
                    'success': False,
                    'data': [],
                    'errors': [f"Unsupported file type: {file_ext}"],
                    'warnings': [],
                    'metadata': {'file_type': file_ext}
                }
            
            # Validate and clean data
            cleaned_data, errors, warnings = self._validate_and_clean_data(data, file_path)
            
            return {
                'success': len(errors) == 0,
                'data': cleaned_data,
                'errors': errors,
                'warnings': warnings,
                'metadata': {
                    'file_type': file_ext,
                    'raw_records': len(data),
                    'valid_records': len(cleaned_data),
                    'file_size': os.path.getsize(file_path)
                }
            }
            
        except Exception as e:
            return {
                'success': False,
                'data': [],
                'errors': [f"Error processing file: {str(e)}"],
                'warnings': [],
                'metadata': {'file_type': file_ext, 'exception': str(e)}
            }
    
    def _read_csv_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Read CSV file"""
        try:
            df = pd.read_csv(
                file_path,
                encoding=self.encoding,
                delimiter=self.delimiter,
                na_values=['', 'NULL', 'null', 'N/A', 'n/a'],
                keep_default_na=True
            )
            return df.to_dict('records')
        except UnicodeDecodeError:
            # Try different encodings
            for encoding in ['latin1', 'cp1252', 'iso-8859-1']:
                try:
                    df = pd.read_csv(
                        file_path,
                        encoding=encoding,
                        delimiter=self.delimiter
                    )
                    self.logger.warning(f"Used {encoding} encoding for {file_path}")
                    return df.to_dict('records')
                except:
                    continue
            raise
    
    def _read_excel_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Read Excel file"""
        # Try to read the first sheet
        df = pd.read_excel(
            file_path,
            na_values=['', 'NULL', 'null', 'N/A', 'n/a'],
            keep_default_na=True
        )
        return df.to_dict('records')
    
    def _read_json_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Read JSON file"""
        with open(file_path, 'r', encoding=self.encoding) as f:
            data = json.load(f)
        
        # Handle different JSON structures
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Look for data array in common locations
            for key in ['data', 'records', 'rows', 'results']:
                if key in data and isinstance(data[key], list):
                    return data[key]
            # If no data array found, treat the dict as a single record
            return [data]
        else:
            raise ValueError(f"Unsupported JSON structure in {file_path}")
    
    def _validate_and_clean_data(self, data: List[Dict[str, Any]], file_path: str) -> tuple:
        """Validate and clean data from file"""
        cleaned_data = []
        errors = []
        warnings = []
        
        if not data:
            errors.append("File contains no data")
            return cleaned_data, errors, warnings
        
        # Check for required columns
        first_record = data[0]
        missing_columns = []
        
        for required_col in self.required_columns:
            if required_col not in first_record:
                missing_columns.append(required_col)
        
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
            return cleaned_data, errors, warnings
        
        # Process each record
        for i, record in enumerate(data):
            try:
                # Apply column mapping
                mapped_record = self._apply_column_mapping(record)
                
                # Clean the record
                cleaned_record = self.clean_record(mapped_record)
                
                # Validate the record
                record_errors = self.validate_record(cleaned_record, i)
                if record_errors:
                    warnings.extend([f"File {os.path.basename(file_path)}, {error}" for error in record_errors])
                    continue
                
                cleaned_data.append(cleaned_record)
                
            except Exception as e:
                warning_msg = f"Error processing record {i}: {str(e)}"
                warnings.append(warning_msg)
        
        return cleaned_data, errors, warnings
    
    def _apply_column_mapping(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Apply column name mapping to record"""
        if not self.column_mapping:
            return record
        
        mapped_record = {}
        
        for original_col, mapped_col in self.column_mapping.items():
            if original_col in record:
                mapped_record[mapped_col] = record[original_col]
        
        # Keep unmapped columns
        for key, value in record.items():
            if key not in self.column_mapping:
                mapped_record[key] = value
        
        return mapped_record
    
    def _move_processed_file(self, file_path: str):
        """Move successfully processed file to processed directory"""
        try:
            if self.create_backup:
                backup_file(file_path, self.processed_directory)
            
            # Move original file
            filename = os.path.basename(file_path)
            destination = os.path.join(self.processed_directory, filename)
            
            # Add timestamp if file already exists
            if os.path.exists(destination):
                name, ext = os.path.splitext(filename)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                destination = os.path.join(self.processed_directory, f"{name}_{timestamp}{ext}")
            
            shutil.move(file_path, destination)
            self.processed_files.add(file_path)
            
            self.logger.info(f"Moved processed file to: {destination}")
            
        except Exception as e:
            self.logger.error(f"Failed to move processed file {file_path}: {str(e)}")
    
    def _move_error_file(self, file_path: str, errors: List[str]):
        """Move file with errors to error directory"""
        try:
            filename = os.path.basename(file_path)
            destination = os.path.join(self.error_directory, filename)
            
            # Add timestamp if file already exists
            if os.path.exists(destination):
                name, ext = os.path.splitext(filename)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                destination = os.path.join(self.error_directory, f"{name}_{timestamp}{ext}")
            
            shutil.move(file_path, destination)
            
            # Create error log file
            error_log_path = destination + '.errors'
            with open(error_log_path, 'w') as f:
                f.write(f"Errors for file: {filename}\n")
                f.write(f"Processed at: {datetime.now().isoformat()}\n\n")
                for error in errors:
                    f.write(f"- {error}\n")
            
            self.logger.info(f"Moved error file to: {destination}")
            
        except Exception as e:
            self.logger.error(f"Failed to move error file {file_path}: {str(e)}")
    
    def get_required_fields(self) -> List[str]:
        """Get required fields for route data"""
        return self.required_columns or ['route_id', 'date']
    
    def standardize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert file record to standard route data format"""
        standardized = {
            'source': 'file',
            'source_name': self.name,
            'collected_at': datetime.now().isoformat(),
            'raw_data': record.copy()
        }
        
        # Apply the same field mapping as API collector
        field_mapping = {
            'route_id': ['route_id', 'id', 'trip_id', 'load_id', 'Route ID', 'Trip ID'],
            'route_date': ['date', 'route_date', 'trip_date', 'load_date', 'Date', 'Route Date'],
            'driver_name': ['driver', 'driver_name', 'Driver', 'Driver Name'],
            'vehicle_id': ['truck', 'vehicle', 'vehicle_id', 'truck_id', 'Vehicle', 'Truck'],
            'customer_name': ['customer', 'customer_name', 'Customer', 'Customer Name'],
            'origin_address': ['pickup', 'origin', 'pickup_address', 'Origin', 'Pickup'],
            'destination_address': ['delivery', 'destination', 'delivery_address', 'Destination'],
            'total_miles': ['miles', 'total_miles', 'distance', 'Miles', 'Distance'],
            'revenue': ['revenue', 'rate', 'pay', 'Revenue', 'Rate'],
            'load_weight': ['weight', 'load_weight', 'Weight', 'Load Weight'],
            'start_time': ['start_time', 'pickup_time', 'Start Time', 'Pickup Time'],
            'end_time': ['end_time', 'delivery_time', 'End Time', 'Delivery Time'],
            'status': ['status', 'Status', 'Trip Status']
        }
        
        for standard_field, possible_fields in field_mapping.items():
            value = None
            for field in possible_fields:
                if field in record and record[field] is not None:
                    value = record[field]
                    break
            
            if value is not None:
                # Apply type conversions based on field
                if standard_field in ['total_miles', 'revenue', 'load_weight']:
                    standardized[standard_field] = safe_float_convert(value)
                elif standard_field in ['route_date', 'start_time', 'end_time']:
                    parsed_date = parse_date(str(value))
                    standardized[standard_field] = parsed_date.isoformat() if parsed_date else None
                elif isinstance(value, str):
                    standardized[standard_field] = clean_string(value)
                else:
                    standardized[standard_field] = value
        
        return standardized