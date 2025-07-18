from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import traceback

from utils.logger import LoggerManager
from utils.helpers import retry_operation

class CollectionStatus(Enum):
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"
    NO_DATA = "no_data"

@dataclass
class CollectionResult:
    status: CollectionStatus
    data: List[Dict[str, Any]]
    errors: List[str]
    warnings: List[str]
    metadata: Dict[str, Any]
    collected_at: datetime
    source_info: Dict[str, Any]
    
    def __post_init__(self):
        if self.collected_at is None:
            self.collected_at = datetime.now()
        if self.data is None:
            self.data = []
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        if self.metadata is None:
            self.metadata = {}
        if self.source_info is None:
            self.source_info = {}

class BaseCollector(ABC):
    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.logger = LoggerManager.get_logger(f'route_pipeline.data_collection.{name}')
        self.last_collection_time = None
        self.collection_count = 0
        self.total_records_collected = 0
        
    @abstractmethod
    def collect_data(self) -> CollectionResult:
        """Collect data from the source"""
        pass
    
    @abstractmethod
    def validate_configuration(self) -> bool:
        """Validate that the collector is properly configured"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test connection to the data source"""
        pass
    
    def get_source_info(self) -> Dict[str, Any]:
        """Get information about the data source"""
        return {
            'name': self.name,
            'type': self.__class__.__name__,
            'config': {k: v for k, v in self.config.items() if 'password' not in k.lower() and 'key' not in k.lower()},
            'last_collection_time': self.last_collection_time,
            'collection_count': self.collection_count,
            'total_records_collected': self.total_records_collected
        }
    
    def collect_with_retry(self, max_attempts: int = 3, delay: float = 1.0) -> CollectionResult:
        """Collect data with retry logic"""
        self.logger.info(f"Starting data collection from {self.name}")
        
        def collection_attempt():
            return self.collect_data()
        
        try:
            result = retry_operation(
                collection_attempt,
                max_attempts=max_attempts,
                delay=delay,
                backoff=2.0
            )
            
            # Update collection statistics
            self.last_collection_time = result.collected_at
            self.collection_count += 1
            self.total_records_collected += len(result.data)
            
            self.logger.info(
                f"Collection completed from {self.name}. "
                f"Status: {result.status.value}, Records: {len(result.data)}, "
                f"Errors: {len(result.errors)}, Warnings: {len(result.warnings)}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Collection failed from {self.name} after {max_attempts} attempts: {str(e)}")
            return CollectionResult(
                status=CollectionStatus.FAILED,
                data=[],
                errors=[f"Collection failed: {str(e)}"],
                warnings=[],
                metadata={'exception': str(e), 'traceback': traceback.format_exc()},
                collected_at=datetime.now(),
                source_info=self.get_source_info()
            )
    
    def validate_collected_data(self, data: List[Dict[str, Any]]) -> List[str]:
        """Validate collected data and return list of validation errors"""
        errors = []
        
        for i, record in enumerate(data):
            record_errors = self.validate_record(record, i)
            errors.extend(record_errors)
        
        return errors
    
    def validate_record(self, record: Dict[str, Any], index: int) -> List[str]:
        """Validate a single record and return list of errors"""
        errors = []
        
        # Basic validation - override in subclasses for specific validation
        if not isinstance(record, dict):
            errors.append(f"Record {index}: Expected dictionary, got {type(record)}")
            return errors
        
        # Check for required fields (override in subclasses)
        required_fields = self.get_required_fields()
        for field in required_fields:
            if field not in record or record[field] is None or record[field] == "":
                errors.append(f"Record {index}: Missing required field '{field}'")
        
        return errors
    
    def get_required_fields(self) -> List[str]:
        """Get list of required fields for this collector"""
        return []
    
    def clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize a single record"""
        cleaned = {}
        
        for key, value in record.items():
            # Clean key names
            clean_key = key.strip().lower().replace(' ', '_').replace('-', '_')
            
            # Clean values
            if isinstance(value, str):
                cleaned[clean_key] = value.strip()
            else:
                cleaned[clean_key] = value
        
        return cleaned
    
    def process_collected_data(self, raw_data: List[Dict[str, Any]]) -> CollectionResult:
        """Process raw collected data into standardized format"""
        processed_data = []
        errors = []
        warnings = []
        
        for i, record in enumerate(raw_data):
            try:
                # Clean the record
                cleaned_record = self.clean_record(record)
                
                # Validate the record
                record_errors = self.validate_record(cleaned_record, i)
                if record_errors:
                    errors.extend(record_errors)
                    continue
                
                # Transform to standard format
                standardized_record = self.standardize_record(cleaned_record)
                processed_data.append(standardized_record)
                
            except Exception as e:
                error_msg = f"Record {i}: Processing error - {str(e)}"
                errors.append(error_msg)
                self.logger.warning(error_msg)
        
        # Determine status
        if not raw_data:
            status = CollectionStatus.NO_DATA
        elif not processed_data:
            status = CollectionStatus.FAILED
        elif len(processed_data) < len(raw_data):
            status = CollectionStatus.PARTIAL_SUCCESS
        else:
            status = CollectionStatus.SUCCESS
        
        return CollectionResult(
            status=status,
            data=processed_data,
            errors=errors,
            warnings=warnings,
            metadata={
                'raw_record_count': len(raw_data),
                'processed_record_count': len(processed_data),
                'error_count': len(errors),
                'warning_count': len(warnings)
            },
            collected_at=datetime.now(),
            source_info=self.get_source_info()
        )
    
    def standardize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert record to standard route data format"""
        # Base implementation - override in subclasses
        return record
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get collection statistics"""
        return {
            'collector_name': self.name,
            'collector_type': self.__class__.__name__,
            'last_collection_time': self.last_collection_time,
            'collection_count': self.collection_count,
            'total_records_collected': self.total_records_collected,
            'configuration_valid': self.validate_configuration(),
            'connection_status': self.test_connection() if self.validate_configuration() else False
        }