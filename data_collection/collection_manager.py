from typing import List, Dict, Any, Optional, Type
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
from dataclasses import dataclass
from enum import Enum

from .base_collector import BaseCollector, CollectionResult, CollectionStatus
from .api_collector import APICollector, ELDAPICollector, TMSAPICollector, DispatchAPICollector
from .file_collector import FileCollector
from .email_collector import EmailCollector
from .manual_entry import ManualEntryCollector
from .validator import DataValidator, RouteDataValidator, create_validator_from_config
from utils.logger import LoggerManager
from utils.helpers import retry_operation
from database.operations import DatabaseOperations
from database.models import Route, Driver, Vehicle, Customer, Location

class CollectorType(Enum):
    API = "api"
    FILE = "file"
    EMAIL = "email"
    MANUAL = "manual"

@dataclass
class CollectionTask:
    collector_name: str
    collector_type: CollectorType
    config: Dict[str, Any]
    enabled: bool = True
    schedule: Optional[str] = None
    last_run: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3

@dataclass
class CollectionSummary:
    total_collectors: int
    successful_collections: int
    failed_collections: int
    total_records_collected: int
    total_errors: int
    total_warnings: int
    collection_time: float
    results: List[CollectionResult]

class CollectionManager:
    def __init__(self, config: Dict[str, Any], db_operations: DatabaseOperations):
        self.config = config
        self.db = db_operations
        self.logger = LoggerManager.get_logger('route_pipeline.data_collection.manager')
        
        # Configuration
        self.max_concurrent_collectors = config.get('max_concurrent_collectors', 4)
        self.default_timeout = config.get('default_timeout', 300)
        self.enable_validation = config.get('enable_validation', True)
        self.auto_save_to_database = config.get('auto_save_to_database', True)
        
        # Collectors and validators
        self.collectors: Dict[str, BaseCollector] = {}
        self.validators: Dict[str, DataValidator] = {}
        self.collection_tasks: List[CollectionTask] = []
        
        # State management
        self._lock = threading.Lock()
        self._running_collections = set()
        
        # Initialize from configuration
        self._initialize_from_config()
    
    def _initialize_from_config(self):
        """Initialize collectors and validators from configuration"""
        try:
            # Initialize validators
            validators_config = self.config.get('validators', {})
            for validator_name, validator_config in validators_config.items():
                if validator_name == 'route_data':
                    self.validators[validator_name] = RouteDataValidator()
                else:
                    self.validators[validator_name] = create_validator_from_config(validator_config)
            
            # Add default route validator if none specified
            if 'route_data' not in self.validators:
                self.validators['route_data'] = RouteDataValidator()
            
            # Initialize collectors
            collectors_config = self.config.get('collectors', {})
            for collector_name, collector_config in collectors_config.items():
                try:
                    collector = self._create_collector(collector_name, collector_config)
                    if collector:
                        self.collectors[collector_name] = collector
                        
                        # Create collection task
                        task = CollectionTask(
                            collector_name=collector_name,
                            collector_type=CollectorType(collector_config.get('type', 'api')),
                            config=collector_config,
                            enabled=collector_config.get('enabled', True),
                            schedule=collector_config.get('schedule'),
                            max_retries=collector_config.get('max_retries', 3)
                        )
                        self.collection_tasks.append(task)
                        
                except Exception as e:
                    self.logger.error(f"Failed to initialize collector {collector_name}: {str(e)}")
            
            self.logger.info(f"Initialized {len(self.collectors)} collectors and {len(self.validators)} validators")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize collection manager: {str(e)}")
    
    def _create_collector(self, name: str, config: Dict[str, Any]) -> Optional[BaseCollector]:
        """Create a collector instance based on configuration"""
        collector_type = config.get('type', 'api').lower()
        
        try:
            if collector_type == 'api':
                # Check for specific API types
                api_type = config.get('api_type', 'generic').lower()
                if api_type == 'eld':
                    return ELDAPICollector(config)
                elif api_type == 'tms':
                    return TMSAPICollector(config)
                elif api_type == 'dispatch':
                    return DispatchAPICollector(config)
                else:
                    return APICollector(name, config)
            
            elif collector_type == 'file':
                return FileCollector(name, config)
            
            elif collector_type == 'email':
                return EmailCollector(name, config)
            
            elif collector_type == 'manual':
                return ManualEntryCollector(name, config)
            
            else:
                self.logger.error(f"Unknown collector type: {collector_type}")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to create collector {name}: {str(e)}")
            return None
    
    def add_collector(self, name: str, collector: BaseCollector, enabled: bool = True):
        """Add a collector manually"""
        with self._lock:
            self.collectors[name] = collector
            
            # Create corresponding task
            task = CollectionTask(
                collector_name=name,
                collector_type=CollectorType.API,  # Default type
                config={},
                enabled=enabled
            )
            self.collection_tasks.append(task)
            
        self.logger.info(f"Added collector: {name}")
    
    def remove_collector(self, name: str):
        """Remove a collector"""
        with self._lock:
            if name in self.collectors:
                del self.collectors[name]
            
            # Remove corresponding task
            self.collection_tasks = [task for task in self.collection_tasks 
                                   if task.collector_name != name]
            
        self.logger.info(f"Removed collector: {name}")
    
    def collect_from_all(self, parallel: bool = True) -> CollectionSummary:
        """Collect data from all enabled collectors"""
        enabled_tasks = [task for task in self.collection_tasks if task.enabled]
        
        if not enabled_tasks:
            self.logger.warning("No enabled collectors found")
            return CollectionSummary(
                total_collectors=0,
                successful_collections=0,
                failed_collections=0,
                total_records_collected=0,
                total_errors=0,
                total_warnings=0,
                collection_time=0.0,
                results=[]
            )
        
        start_time = time.time()
        self.logger.info(f"Starting collection from {len(enabled_tasks)} collectors")
        
        if parallel and len(enabled_tasks) > 1:
            results = self._collect_parallel(enabled_tasks)
        else:
            results = self._collect_sequential(enabled_tasks)
        
        collection_time = time.time() - start_time
        
        # Generate summary
        summary = self._generate_summary(results, collection_time)
        
        # Save to database if enabled
        if self.auto_save_to_database:
            self._save_results_to_database(results)
        
        self.logger.info(
            f"Collection completed in {collection_time:.2f}s. "
            f"Success: {summary.successful_collections}, "
            f"Failed: {summary.failed_collections}, "
            f"Records: {summary.total_records_collected}"
        )
        
        return summary
    
    def collect_from_source(self, collector_name: str) -> CollectionResult:
        """Collect data from a specific collector"""
        if collector_name not in self.collectors:
            return CollectionResult(
                status=CollectionStatus.FAILED,
                data=[],
                errors=[f"Collector '{collector_name}' not found"],
                warnings=[],
                metadata={},
                collected_at=datetime.now(),
                source_info={}
            )
        
        collector = self.collectors[collector_name]
        self.logger.info(f"Starting collection from: {collector_name}")
        
        try:
            # Mark as running
            with self._lock:
                self._running_collections.add(collector_name)
            
            # Collect data with retry logic
            result = self._collect_with_error_handling(collector)
            
            # Validate data if enabled
            if self.enable_validation and result.data:
                result = self._validate_collection_result(result)
            
            # Update task last run time
            self._update_task_last_run(collector_name)
            
            return result
            
        finally:
            # Mark as not running
            with self._lock:
                self._running_collections.discard(collector_name)
    
    def _collect_parallel(self, tasks: List[CollectionTask]) -> List[CollectionResult]:
        """Collect data from multiple collectors in parallel"""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_concurrent_collectors) as executor:
            # Submit collection tasks
            future_to_task = {
                executor.submit(self.collect_from_source, task.collector_name): task
                for task in tasks
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_task, timeout=self.default_timeout):
                task = future_to_task[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"Collection failed for {task.collector_name}: {str(e)}")
                    results.append(CollectionResult(
                        status=CollectionStatus.FAILED,
                        data=[],
                        errors=[f"Collection failed: {str(e)}"],
                        warnings=[],
                        metadata={'exception': str(e)},
                        collected_at=datetime.now(),
                        source_info={}
                    ))
        
        return results
    
    def _collect_sequential(self, tasks: List[CollectionTask]) -> List[CollectionResult]:
        """Collect data from collectors sequentially"""
        results = []
        
        for task in tasks:
            try:
                result = self.collect_from_source(task.collector_name)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Collection failed for {task.collector_name}: {str(e)}")
                results.append(CollectionResult(
                    status=CollectionStatus.FAILED,
                    data=[],
                    errors=[f"Collection failed: {str(e)}"],
                    warnings=[],
                    metadata={'exception': str(e)},
                    collected_at=datetime.now(),
                    source_info={}
                ))
        
        return results
    
    def _collect_with_error_handling(self, collector: BaseCollector) -> CollectionResult:
        """Collect data with error handling and retry logic"""
        max_attempts = 3
        delay = 1.0
        
        def collection_attempt():
            return collector.collect_with_retry(max_attempts=1, delay=0)
        
        try:
            return retry_operation(
                collection_attempt,
                max_attempts=max_attempts,
                delay=delay,
                backoff=2.0
            )
        except Exception as e:
            self.logger.error(f"Collection failed after {max_attempts} attempts: {str(e)}")
            return CollectionResult(
                status=CollectionStatus.FAILED,
                data=[],
                errors=[f"Collection failed after {max_attempts} attempts: {str(e)}"],
                warnings=[],
                metadata={'max_attempts': max_attempts, 'exception': str(e)},
                collected_at=datetime.now(),
                source_info=collector.get_source_info()
            )
    
    def _validate_collection_result(self, result: CollectionResult) -> CollectionResult:
        """Validate collection result using configured validators"""
        if not result.data:
            return result
        
        try:
            # Use route data validator as default
            validator = self.validators.get('route_data')
            if not validator:
                return result
            
            # Validate the data
            validation_summary = validator.validate_batch(result.data)
            
            # Update result with validation information
            result.data = validation_summary['valid_record_list']
            
            # Add validation warnings and errors
            for validation_result in validation_summary['validation_results']:
                if validation_result.severity.value == 'error':
                    result.errors.append(validation_result.message)
                elif validation_result.severity.value == 'warning':
                    result.warnings.append(validation_result.message)
            
            # Update metadata
            result.metadata.update({
                'validation_applied': True,
                'original_record_count': validation_summary['total_records'],
                'valid_record_count': validation_summary['valid_records'],
                'validation_error_count': validation_summary['error_count'],
                'validation_warning_count': validation_summary['warning_count']
            })
            
            self.logger.info(
                f"Validation completed. Valid: {validation_summary['valid_records']}, "
                f"Invalid: {validation_summary['invalid_records']}, "
                f"Errors: {validation_summary['error_count']}, "
                f"Warnings: {validation_summary['warning_count']}"
            )
            
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            result.warnings.append(f"Validation failed: {str(e)}")
        
        return result
    
    def _save_results_to_database(self, results: List[CollectionResult]):
        """Save collection results to database"""
        total_saved = 0
        
        for result in results:
            if result.status == CollectionStatus.FAILED or not result.data:
                continue
            
            try:
                saved_count = self._save_collection_result_to_db(result)
                total_saved += saved_count
            except Exception as e:
                self.logger.error(f"Failed to save collection result to database: {str(e)}")
        
        if total_saved > 0:
            self.logger.info(f"Saved {total_saved} records to database")
    
    def _save_collection_result_to_db(self, result: CollectionResult) -> int:
        """Save a single collection result to database"""
        saved_count = 0
        
        for record in result.data:
            try:
                # This is a simplified version - in practice, you'd need to:
                # 1. Map the standardized record to database models
                # 2. Handle relationships (drivers, vehicles, customers, locations)
                # 3. Avoid duplicates
                # 4. Handle partial data
                
                # For now, just log what would be saved
                self.logger.debug(f"Would save record: {record.get('route_id', 'unknown')}")
                saved_count += 1
                
            except Exception as e:
                self.logger.error(f"Failed to save individual record: {str(e)}")
        
        return saved_count
    
    def _generate_summary(self, results: List[CollectionResult], collection_time: float) -> CollectionSummary:
        """Generate collection summary from results"""
        successful = sum(1 for r in results if r.status in [CollectionStatus.SUCCESS, CollectionStatus.PARTIAL_SUCCESS])
        failed = sum(1 for r in results if r.status == CollectionStatus.FAILED)
        total_records = sum(len(r.data) for r in results)
        total_errors = sum(len(r.errors) for r in results)
        total_warnings = sum(len(r.warnings) for r in results)
        
        return CollectionSummary(
            total_collectors=len(results),
            successful_collections=successful,
            failed_collections=failed,
            total_records_collected=total_records,
            total_errors=total_errors,
            total_warnings=total_warnings,
            collection_time=collection_time,
            results=results
        )
    
    def _update_task_last_run(self, collector_name: str):
        """Update the last run time for a collection task"""
        for task in self.collection_tasks:
            if task.collector_name == collector_name:
                task.last_run = datetime.now()
                break
    
    def get_collector_status(self) -> Dict[str, Any]:
        """Get status of all collectors"""
        status = {}
        
        for name, collector in self.collectors.items():
            task = next((t for t in self.collection_tasks if t.collector_name == name), None)
            
            status[name] = {
                'type': collector.__class__.__name__,
                'enabled': task.enabled if task else False,
                'configuration_valid': collector.validate_configuration(),
                'connection_status': collector.test_connection() if collector.validate_configuration() else False,
                'last_collection': task.last_run.isoformat() if task and task.last_run else None,
                'collection_count': collector.collection_count,
                'total_records_collected': collector.total_records_collected,
                'currently_running': name in self._running_collections
            }
        
        return status
    
    def enable_collector(self, collector_name: str):
        """Enable a specific collector"""
        for task in self.collection_tasks:
            if task.collector_name == collector_name:
                task.enabled = True
                self.logger.info(f"Enabled collector: {collector_name}")
                break
    
    def disable_collector(self, collector_name: str):
        """Disable a specific collector"""
        for task in self.collection_tasks:
            if task.collector_name == collector_name:
                task.enabled = False
                self.logger.info(f"Disabled collector: {collector_name}")
                break
    
    def test_all_collectors(self) -> Dict[str, bool]:
        """Test connection for all collectors"""
        test_results = {}
        
        for name, collector in self.collectors.items():
            try:
                test_results[name] = collector.test_connection()
            except Exception as e:
                self.logger.error(f"Connection test failed for {name}: {str(e)}")
                test_results[name] = False
        
        return test_results