import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Optional

class Logger:
    def __init__(self, name: str, log_file: str = None, log_level: str = 'INFO', 
                 max_log_size: int = 10485760, backup_count: int = 5):
        self.name = name
        self.log_file = log_file
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        self.max_log_size = max_log_size
        self.backup_count = backup_count
        self.logger = None
        self.setup_logger()
    
    def setup_logger(self):
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.log_level)
        
        # Clear existing handlers
        self.logger.handlers = []
        
        # Create formatters
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (if log_file is specified)
        if self.log_file:
            # Ensure log directory exists
            os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
            
            file_handler = RotatingFileHandler(
                self.log_file,
                maxBytes=self.max_log_size,
                backupCount=self.backup_count
            )
            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
    
    def info(self, message: str, *args, **kwargs):
        self.logger.info(message, *args, **kwargs)
    
    def debug(self, message: str, *args, **kwargs):
        self.logger.debug(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs):
        self.logger.warning(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs):
        self.logger.error(message, *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs):
        self.logger.critical(message, *args, **kwargs)
    
    def exception(self, message: str, *args, **kwargs):
        self.logger.exception(message, *args, **kwargs)

class LoggerManager:
    _loggers = {}
    
    @classmethod
    def get_logger(cls, name: str, log_file: str = None, log_level: str = 'INFO',
                   max_log_size: int = 10485760, backup_count: int = 5) -> Logger:
        if name not in cls._loggers:
            cls._loggers[name] = Logger(name, log_file, log_level, max_log_size, backup_count)
        return cls._loggers[name]
    
    @classmethod
    def setup_application_logging(cls, config):
        """Setup logging for the entire application based on configuration"""
        log_file = config.log_file if hasattr(config, 'log_file') else None
        log_level = config.log_level if hasattr(config, 'log_level') else 'INFO'
        max_log_size = config.max_log_size if hasattr(config, 'max_log_size') else 10485760
        backup_count = config.backup_count if hasattr(config, 'backup_count') else 5
        
        # Create main application logger
        main_logger = cls.get_logger(
            'route_pipeline',
            log_file=log_file,
            log_level=log_level,
            max_log_size=max_log_size,
            backup_count=backup_count
        )
        
        # Create component-specific loggers
        component_loggers = {
            'data_collection': cls.get_logger('route_pipeline.data_collection', log_file, log_level, max_log_size, backup_count),
            'data_processing': cls.get_logger('route_pipeline.data_processing', log_file, log_level, max_log_size, backup_count),
            'database': cls.get_logger('route_pipeline.database', log_file, log_level, max_log_size, backup_count),
            'reporting': cls.get_logger('route_pipeline.reporting', log_file, log_level, max_log_size, backup_count),
            'automation': cls.get_logger('route_pipeline.automation', log_file, log_level, max_log_size, backup_count),
        }
        
        return main_logger, component_loggers

def create_operation_logger(operation_name: str, log_file: Optional[str] = None) -> Logger:
    """Create a logger for a specific operation with timestamped log file"""
    if log_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f'logs/{operation_name}_{timestamp}.log'
    
    return LoggerManager.get_logger(f'route_pipeline.{operation_name}', log_file)

def log_function_call(func):
    """Decorator to log function calls with parameters and execution time"""
    def wrapper(*args, **kwargs):
        logger = LoggerManager.get_logger('route_pipeline.function_calls')
        start_time = datetime.now()
        
        logger.info(f"Calling {func.__name__} with args={args}, kwargs={kwargs}")
        
        try:
            result = func(*args, **kwargs)
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"{func.__name__} completed successfully in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"{func.__name__} failed after {execution_time:.2f} seconds with error: {str(e)}")
            raise
    
    return wrapper

def log_database_operation(operation_type: str, table_name: str, record_id: Optional[int] = None):
    """Log database operations for auditing purposes"""
    logger = LoggerManager.get_logger('route_pipeline.database.audit')
    
    if record_id:
        logger.info(f"Database {operation_type} on table '{table_name}' for record ID {record_id}")
    else:
        logger.info(f"Database {operation_type} on table '{table_name}'")

def log_error_with_context(logger: Logger, error: Exception, context: dict = None):
    """Log an error with additional context information"""
    error_msg = f"Error occurred: {str(error)}"
    
    if context:
        context_str = ", ".join([f"{k}={v}" for k, v in context.items()])
        error_msg += f" | Context: {context_str}"
    
    logger.error(error_msg)
    logger.exception("Exception details:")

# Create a default logger instance
default_logger = LoggerManager.get_logger('route_pipeline')

# Convenience function for getting loggers
def get_logger(name: str, log_file: str = None, log_level: str = 'INFO',
               max_log_size: int = 10485760, backup_count: int = 5) -> Logger:
    """Get a logger instance with the specified parameters."""
    return LoggerManager.get_logger(name, log_file, log_level, max_log_size, backup_count)