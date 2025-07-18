import requests
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import time

from .base_collector import BaseCollector, CollectionResult, CollectionStatus
from utils.helpers import safe_float_convert, safe_int_convert, parse_date, clean_string

class APICollector(BaseCollector):
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        
        # API configuration
        self.base_url = config.get('base_url', '')
        self.api_key = config.get('api_key', '')
        self.api_secret = config.get('api_secret', '')
        self.timeout = config.get('timeout', 30)
        self.rate_limit_delay = config.get('rate_limit_delay', 1.0)
        self.headers = config.get('headers', {})
        self.auth_type = config.get('auth_type', 'api_key')  # api_key, bearer, basic
        
        # Endpoints for different data types
        self.endpoints = config.get('endpoints', {})
        
        # Data filtering
        self.date_field = config.get('date_field', 'date')
        self.max_records_per_request = config.get('max_records_per_request', 1000)
        
        self.session = requests.Session()
        self._setup_authentication()
    
    def _setup_authentication(self):
        """Setup authentication headers based on auth type"""
        if self.auth_type == 'api_key' and self.api_key:
            self.headers['X-API-Key'] = self.api_key
        elif self.auth_type == 'bearer' and self.api_key:
            self.headers['Authorization'] = f'Bearer {self.api_key}'
        elif self.auth_type == 'basic' and self.api_key and self.api_secret:
            import base64
            credentials = base64.b64encode(f"{self.api_key}:{self.api_secret}".encode()).decode()
            self.headers['Authorization'] = f'Basic {credentials}'
        
        # Set default headers
        self.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'RouteDataPipeline/1.0'
        })
        
        self.session.headers.update(self.headers)
    
    def validate_configuration(self) -> bool:
        """Validate API collector configuration"""
        if not self.base_url:
            self.logger.error("Base URL is required")
            return False
        
        if not self.api_key:
            self.logger.error("API key is required")
            return False
        
        if not self.endpoints:
            self.logger.warning("No endpoints configured")
        
        try:
            urlparse(self.base_url)
        except Exception:
            self.logger.error("Invalid base URL format")
            return False
        
        return True
    
    def test_connection(self) -> bool:
        """Test connection to the API"""
        if not self.validate_configuration():
            return False
        
        try:
            # Try to hit a health/status endpoint or the base URL
            test_url = self.endpoints.get('health', self.base_url)
            if not test_url.startswith('http'):
                test_url = urljoin(self.base_url, test_url)
            
            response = self.session.get(test_url, timeout=self.timeout)
            
            if response.status_code == 200:
                self.logger.info(f"API connection test successful for {self.name}")
                return True
            elif response.status_code == 401:
                self.logger.error(f"API authentication failed for {self.name}")
                return False
            else:
                self.logger.warning(f"API connection test returned status {response.status_code} for {self.name}")
                return response.status_code < 500
                
        except requests.exceptions.Timeout:
            self.logger.error(f"API connection timeout for {self.name}")
            return False
        except requests.exceptions.ConnectionError:
            self.logger.error(f"API connection error for {self.name}")
            return False
        except Exception as e:
            self.logger.error(f"API connection test failed for {self.name}: {str(e)}")
            return False
    
    def collect_data(self) -> CollectionResult:
        """Collect data from API endpoints"""
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
        metadata = {}
        
        # Collect from each configured endpoint
        for endpoint_name, endpoint_config in self.endpoints.items():
            if endpoint_name == 'health':
                continue
                
            self.logger.info(f"Collecting data from endpoint: {endpoint_name}")
            
            try:
                endpoint_data = self._collect_from_endpoint(endpoint_name, endpoint_config)
                all_data.extend(endpoint_data['data'])
                all_errors.extend(endpoint_data['errors'])
                all_warnings.extend(endpoint_data['warnings'])
                metadata[endpoint_name] = endpoint_data['metadata']
                
                # Rate limiting
                if self.rate_limit_delay > 0:
                    time.sleep(self.rate_limit_delay)
                    
            except Exception as e:
                error_msg = f"Failed to collect from endpoint {endpoint_name}: {str(e)}"
                all_errors.append(error_msg)
                self.logger.error(error_msg)
        
        return self.process_collected_data(all_data)
    
    def _collect_from_endpoint(self, endpoint_name: str, endpoint_config: Dict[str, Any]) -> Dict[str, Any]:
        """Collect data from a specific endpoint"""
        url = endpoint_config.get('url', '')
        if not url.startswith('http'):
            url = urljoin(self.base_url, url)
        
        method = endpoint_config.get('method', 'GET').upper()
        params = endpoint_config.get('params', {})
        data_path = endpoint_config.get('data_path', 'data')  # JSON path to data array
        
        collected_data = []
        errors = []
        warnings = []
        metadata = {'endpoint': endpoint_name, 'url': url, 'method': method}
        
        try:
            # Add date filtering if specified
            if 'date_filter' in endpoint_config:
                date_filter = endpoint_config['date_filter']
                if date_filter.get('enabled', False):
                    from_date = datetime.now() - timedelta(days=date_filter.get('days_back', 7))
                    params[date_filter.get('param_name', 'from_date')] = from_date.strftime('%Y-%m-%d')
            
            # Make the API request
            if method == 'GET':
                response = self.session.get(url, params=params, timeout=self.timeout)
            elif method == 'POST':
                response = self.session.post(url, json=params, timeout=self.timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            
            # Parse response
            response_data = response.json()
            
            # Extract data based on data_path
            if data_path:
                data = self._extract_data_by_path(response_data, data_path)
            else:
                data = response_data
            
            if isinstance(data, list):
                collected_data = data
            elif isinstance(data, dict):
                collected_data = [data]
            else:
                warnings.append(f"Unexpected data type from {endpoint_name}: {type(data)}")
            
            metadata.update({
                'status_code': response.status_code,
                'response_size': len(response.content),
                'records_count': len(collected_data)
            })
            
            self.logger.info(f"Collected {len(collected_data)} records from {endpoint_name}")
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP error from {endpoint_name}: {e.response.status_code} - {e.response.text}"
            errors.append(error_msg)
            self.logger.error(error_msg)
        except requests.exceptions.Timeout:
            error_msg = f"Timeout error from {endpoint_name}"
            errors.append(error_msg)
            self.logger.error(error_msg)
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error from {endpoint_name}: {str(e)}"
            errors.append(error_msg)
            self.logger.error(error_msg)
        except json.JSONDecodeError as e:
            error_msg = f"JSON decode error from {endpoint_name}: {str(e)}"
            errors.append(error_msg)
            self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error from {endpoint_name}: {str(e)}"
            errors.append(error_msg)
            self.logger.error(error_msg)
        
        return {
            'data': collected_data,
            'errors': errors,
            'warnings': warnings,
            'metadata': metadata
        }
    
    def _extract_data_by_path(self, data: Dict[str, Any], path: str) -> Any:
        """Extract data from nested JSON using dot notation path"""
        current = data
        
        for key in path.split('.'):
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return []
        
        return current
    
    def get_required_fields(self) -> List[str]:
        """Get required fields for route data"""
        return ['route_id', 'date']
    
    def standardize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert API record to standard route data format"""
        standardized = {
            'source': 'api',
            'source_name': self.name,
            'collected_at': datetime.now().isoformat(),
            'raw_data': record.copy()
        }
        
        # Map common field names to standard format
        field_mapping = {
            'route_id': ['route_id', 'id', 'trip_id', 'load_id'],
            'route_date': ['date', 'route_date', 'trip_date', 'load_date', 'dispatch_date'],
            'driver_name': ['driver', 'driver_name', 'driver_id'],
            'vehicle_id': ['truck', 'vehicle', 'vehicle_id', 'truck_id'],
            'customer_name': ['customer', 'customer_name', 'shipper', 'consignee'],
            'origin_address': ['pickup', 'origin', 'pickup_address', 'origin_address'],
            'destination_address': ['delivery', 'destination', 'delivery_address', 'destination_address'],
            'total_miles': ['miles', 'total_miles', 'distance', 'total_distance'],
            'revenue': ['revenue', 'rate', 'pay', 'total_pay'],
            'load_weight': ['weight', 'load_weight', 'cargo_weight'],
            'start_time': ['start_time', 'pickup_time', 'departure_time'],
            'end_time': ['end_time', 'delivery_time', 'arrival_time'],
            'status': ['status', 'trip_status', 'load_status']
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

# Specific API collector implementations
class ELDAPICollector(APICollector):
    """Electronic Logging Device API collector"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__('ELD_API', config)
    
    def get_required_fields(self) -> List[str]:
        return ['vehicle_id', 'driver_id', 'start_time', 'total_miles']

class TMSAPICollector(APICollector):
    """Transportation Management System API collector"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__('TMS_API', config)
    
    def get_required_fields(self) -> List[str]:
        return ['load_id', 'pickup_date', 'delivery_date', 'revenue']

class DispatchAPICollector(APICollector):
    """Dispatch system API collector"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__('DISPATCH_API', config)
    
    def get_required_fields(self) -> List[str]:
        return ['trip_id', 'driver_name', 'vehicle_id', 'route_date']