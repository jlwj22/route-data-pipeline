import os
import configparser
from typing import Dict, Any, Optional
from pathlib import Path

class Settings:
    def __init__(self, config_file: Optional[str] = None):
        self.config = configparser.ConfigParser()
        
        if config_file is None:
            config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
        
        self.config_file = config_file
        self.load_config()
    
    def load_config(self):
        if os.path.exists(self.config_file):
            self.config.read(self.config_file)
        else:
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
    
    def get(self, section: str, key: str, fallback: Any = None) -> Any:
        try:
            return self.config.get(section, key, fallback=fallback)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return fallback
    
    def getint(self, section: str, key: str, fallback: int = 0) -> int:
        try:
            return self.config.getint(section, key, fallback=fallback)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
            return fallback
    
    def getfloat(self, section: str, key: str, fallback: float = 0.0) -> float:
        try:
            return self.config.getfloat(section, key, fallback=fallback)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
            return fallback
    
    def getboolean(self, section: str, key: str, fallback: bool = False) -> bool:
        try:
            return self.config.getboolean(section, key, fallback=fallback)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
            return fallback
    
    def get_section(self, section: str) -> Dict[str, str]:
        try:
            return dict(self.config.items(section))
        except configparser.NoSectionError:
            return {}
    
    def set(self, section: str, key: str, value: str):
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, key, str(value))
    
    def save(self):
        with open(self.config_file, 'w') as configfile:
            self.config.write(configfile)
    
    @property
    def database_path(self) -> str:
        return self.get('DATABASE', 'db_path', 'data/route_pipeline.db')
    
    @property
    def backup_path(self) -> str:
        return self.get('DATABASE', 'backup_path', 'data/archive/')
    
    @property
    def backup_interval(self) -> int:
        return self.getint('DATABASE', 'backup_interval', 24)
    
    @property
    def log_level(self) -> str:
        return self.get('LOGGING', 'log_level', 'INFO')
    
    @property
    def log_file(self) -> str:
        return self.get('LOGGING', 'log_file', 'logs/route_pipeline.log')
    
    @property
    def max_log_size(self) -> int:
        return self.getint('LOGGING', 'max_log_size', 10485760)
    
    @property
    def backup_count(self) -> int:
        return self.getint('LOGGING', 'backup_count', 5)
    
    @property
    def api_timeout(self) -> int:
        return self.getint('API', 'api_timeout', 30)
    
    @property
    def retry_attempts(self) -> int:
        return self.getint('API', 'retry_attempts', 3)
    
    @property
    def retry_delay(self) -> int:
        return self.getint('API', 'retry_delay', 5)
    
    @property
    def data_collection_interval(self) -> int:
        return self.getint('SCHEDULING', 'data_collection_interval', 3600)
    
    @property
    def report_generation_time(self) -> str:
        return self.get('SCHEDULING', 'report_generation_time', '08:00')
    
    @property
    def backup_time(self) -> str:
        return self.get('SCHEDULING', 'backup_time', '02:00')
    
    @property
    def email_config(self) -> Dict[str, str]:
        return self.get_section('EMAIL')
    
    @property
    def output_directory(self) -> str:
        return self.get('REPORTS', 'output_directory', 'data/output/')
    
    @property
    def archive_directory(self) -> str:
        return self.get('REPORTS', 'archive_directory', 'data/archive/reports/')
    
    @property
    def default_format(self) -> str:
        return self.get('REPORTS', 'default_format', 'excel')
    
    @property
    def include_charts(self) -> bool:
        return self.getboolean('REPORTS', 'include_charts', True)
    
    @property
    def geocoding_service(self) -> str:
        return self.get('GEOCODING', 'geocoding_service', 'nominatim')
    
    @property
    def cache_geocoding_results(self) -> bool:
        return self.getboolean('GEOCODING', 'cache_geocoding_results', True)
    
    @property
    def geocoding_timeout(self) -> int:
        return self.getint('GEOCODING', 'geocoding_timeout', 10)
    
    @property
    def max_concurrent_processes(self) -> int:
        return self.getint('PERFORMANCE', 'max_concurrent_processes', 4)
    
    @property
    def database_connection_pool_size(self) -> int:
        return self.getint('PERFORMANCE', 'database_connection_pool_size', 10)
    
    @property
    def report_generation_timeout(self) -> int:
        return self.getint('PERFORMANCE', 'report_generation_timeout', 300)
    
    @property
    def config_dir(self) -> str:
        return os.path.dirname(self.config_file)
    
    def get_value(self, section: str, key: str, fallback: Any = None) -> Any:
        """Get a configuration value with fallback"""
        return self.get(section, key, fallback)

    def get_google_maps_api_key(self) -> Optional[str]:
        """Get Google Maps API key from config or environment."""
        api_key = self.get('API_KEYS', 'google_maps_api_key', '')
        if not api_key:
            api_key = os.environ.get('GOOGLE_MAPS_API_KEY', '')
        return api_key if api_key else None

    def get_mapbox_api_key(self) -> Optional[str]:
        """Get MapBox API key from config or environment."""
        api_key = self.get('API_KEYS', 'mapbox_api_key', '')
        if not api_key:
            api_key = os.environ.get('MAPBOX_API_KEY', '')
        return api_key if api_key else None

    def ensure_directories(self):
        directories = [
            self.output_directory,
            self.archive_directory,
            self.backup_path,
            os.path.dirname(self.log_file),
            os.path.dirname(self.database_path)
        ]
        
        for directory in directories:
            if directory:
                Path(directory).mkdir(parents=True, exist_ok=True)

settings = Settings()