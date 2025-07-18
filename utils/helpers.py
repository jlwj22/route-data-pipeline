import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from pathlib import Path
import hashlib
import shutil
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
import time

def ensure_directory_exists(directory_path: str) -> None:
    """Ensure a directory exists, create it if it doesn't"""
    Path(directory_path).mkdir(parents=True, exist_ok=True)

def get_file_hash(file_path: str) -> str:
    """Calculate MD5 hash of a file"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def backup_file(source_path: str, backup_directory: str, add_timestamp: bool = True) -> str:
    """Create a backup of a file"""
    ensure_directory_exists(backup_directory)
    
    filename = os.path.basename(source_path)
    name, ext = os.path.splitext(filename)
    
    if add_timestamp:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f"{name}_{timestamp}{ext}"
    else:
        backup_filename = filename
    
    backup_path = os.path.join(backup_directory, backup_filename)
    shutil.copy2(source_path, backup_path)
    
    return backup_path

def clean_string(value: str) -> str:
    """Clean and normalize string values"""
    if not value:
        return ""
    
    # Remove extra whitespace and normalize
    cleaned = str(value).strip()
    # Remove multiple spaces
    cleaned = ' '.join(cleaned.split())
    
    return cleaned

def safe_float_convert(value: Any) -> Optional[float]:
    """Safely convert a value to float, return None if conversion fails"""
    if value is None or value == "":
        return None
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_int_convert(value: Any) -> Optional[int]:
    """Safely convert a value to int, return None if conversion fails"""
    if value is None or value == "":
        return None
    
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def parse_date(date_str: str, formats: List[str] = None) -> Optional[datetime]:
    """Parse date string with multiple format attempts"""
    if not date_str:
        return None
    
    if formats is None:
        formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f'
        ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in miles"""
    try:
        point1 = (lat1, lon1)
        point2 = (lat2, lon2)
        distance = geodesic(point1, point2).miles
        return round(distance, 2)
    except Exception:
        return 0.0

def calculate_duration(start_time: datetime, end_time: datetime) -> Dict[str, float]:
    """Calculate duration between two datetime objects"""
    if not start_time or not end_time:
        return {'hours': 0, 'minutes': 0, 'seconds': 0}
    
    duration = end_time - start_time
    total_seconds = duration.total_seconds()
    
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    
    return {
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds,
        'total_seconds': total_seconds,
        'total_minutes': total_seconds / 60,
        'total_hours': total_seconds / 3600
    }

def calculate_efficiency_metrics(route_data: Dict[str, Any]) -> Dict[str, float]:
    """Calculate various efficiency metrics for a route"""
    metrics = {}
    
    # Miles per gallon
    if route_data.get('total_miles') and route_data.get('fuel_consumed'):
        metrics['mpg'] = route_data['total_miles'] / route_data['fuel_consumed']
    
    # Revenue per mile
    if route_data.get('revenue') and route_data.get('total_miles'):
        metrics['revenue_per_mile'] = route_data['revenue'] / route_data['total_miles']
    
    # Cost per mile
    total_cost = sum([
        route_data.get('fuel_cost', 0),
        route_data.get('toll_cost', 0),
        route_data.get('driver_pay', 0),
        route_data.get('other_costs', 0)
    ])
    if total_cost and route_data.get('total_miles'):
        metrics['cost_per_mile'] = total_cost / route_data['total_miles']
    
    # Profit margin
    if route_data.get('revenue') and total_cost:
        profit = route_data['revenue'] - total_cost
        metrics['profit_margin'] = (profit / route_data['revenue']) * 100
    
    # Deadhead percentage
    if route_data.get('empty_miles') and route_data.get('total_miles'):
        metrics['deadhead_percentage'] = (route_data['empty_miles'] / route_data['total_miles']) * 100
    
    return metrics

def format_currency(amount: float) -> str:
    """Format currency values"""
    if amount is None:
        return "$0.00"
    return f"${amount:,.2f}"

def format_percentage(value: float) -> str:
    """Format percentage values"""
    if value is None:
        return "0.00%"
    return f"{value:.2f}%"

def format_miles(miles: float) -> str:
    """Format miles values"""
    if miles is None:
        return "0.0 mi"
    return f"{miles:,.1f} mi"

def validate_email(email: str) -> bool:
    """Basic email validation"""
    if not email:
        return False
    
    return '@' in email and '.' in email.split('@')[1]

def validate_phone(phone: str) -> bool:
    """Basic phone number validation"""
    if not phone:
        return False
    
    # Remove all non-digit characters
    digits = ''.join(filter(str.isdigit, phone))
    
    # Check if it's a valid US phone number (10 digits)
    return len(digits) == 10

def normalize_address(address: str, city: str, state: str, zip_code: str = "") -> str:
    """Normalize address format"""
    components = [
        clean_string(address),
        clean_string(city),
        clean_string(state),
        clean_string(zip_code)
    ]
    
    return ", ".join([c for c in components if c])

def geocode_address(address: str, timeout: int = 10) -> Optional[Tuple[float, float]]:
    """Geocode an address to get latitude and longitude"""
    try:
        geolocator = Nominatim(user_agent="route_pipeline")
        location = geolocator.geocode(address, timeout=timeout)
        
        if location:
            return (location.latitude, location.longitude)
        
        return None
    except Exception:
        return None

def retry_operation(func, max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Retry an operation with exponential backoff"""
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as e:
            if attempt == max_attempts - 1:
                raise e
            
            time.sleep(delay * (backoff ** attempt))

def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split a list into chunks of specified size"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def get_business_days(start_date: datetime, end_date: datetime) -> List[datetime]:
    """Get list of business days between two dates"""
    business_days = []
    current_date = start_date
    
    while current_date <= end_date:
        if current_date.weekday() < 5:  # Monday = 0, Sunday = 6
            business_days.append(current_date)
        current_date += timedelta(days=1)
    
    return business_days

def export_to_csv(data: List[Dict[str, Any]], file_path: str) -> None:
    """Export data to CSV file"""
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)

def export_to_json(data: Any, file_path: str) -> None:
    """Export data to JSON file"""
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2, default=str)

def load_json_file(file_path: str) -> Any:
    """Load data from JSON file"""
    with open(file_path, 'r') as f:
        return json.load(f)

def get_file_size(file_path: str) -> int:
    """Get file size in bytes"""
    return os.path.getsize(file_path)

def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"

def get_system_info() -> Dict[str, Any]:
    """Get system information"""
    import platform
    import psutil
    
    return {
        'platform': platform.system(),
        'platform_version': platform.version(),
        'python_version': platform.python_version(),
        'cpu_count': psutil.cpu_count(),
        'memory_total': psutil.virtual_memory().total,
        'memory_available': psutil.virtual_memory().available,
        'disk_usage': psutil.disk_usage('/').percent
    }

def create_error_summary(errors: List[Exception]) -> Dict[str, Any]:
    """Create a summary of errors"""
    error_types = {}
    
    for error in errors:
        error_type = type(error).__name__
        if error_type not in error_types:
            error_types[error_type] = {
                'count': 0,
                'messages': []
            }
        
        error_types[error_type]['count'] += 1
        error_types[error_type]['messages'].append(str(error))
    
    return {
        'total_errors': len(errors),
        'error_types': error_types,
        'most_common_error': max(error_types.keys(), key=lambda x: error_types[x]['count']) if error_types else None
    }

def time_execution(func):
    """Decorator to measure execution time"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"{func.__name__} executed in {execution_time:.2f} seconds")
        return result
    
    return wrapper