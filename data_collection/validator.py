from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, date
from dataclasses import dataclass
from enum import Enum
import re
from decimal import Decimal, InvalidOperation

from utils.helpers import safe_float_convert, safe_int_convert, parse_date, validate_email, validate_phone
from utils.logger import LoggerManager

class ValidationSeverity(Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

@dataclass
class ValidationRule:
    field_name: str
    rule_type: str
    parameters: Dict[str, Any]
    severity: ValidationSeverity
    message: str
    custom_validator: Optional[Callable] = None

@dataclass
class ValidationResult:
    field_name: str
    rule_type: str
    severity: ValidationSeverity
    message: str
    value: Any
    is_valid: bool
    suggested_value: Any = None

class DataValidator:
    def __init__(self, name: str = "default"):
        self.name = name
        self.logger = LoggerManager.get_logger(f'route_pipeline.data_collection.validator.{name}')
        self.rules = []
        self.custom_validators = {}
        
        # Register built-in validators
        self._register_builtin_validators()
    
    def add_rule(self, rule: ValidationRule):
        """Add a validation rule"""
        self.rules.append(rule)
        self.logger.debug(f"Added validation rule: {rule.field_name} - {rule.rule_type}")
    
    def add_custom_validator(self, name: str, validator_func: Callable):
        """Add a custom validator function"""
        self.custom_validators[name] = validator_func
        self.logger.debug(f"Added custom validator: {name}")
    
    def validate_record(self, record: Dict[str, Any], record_index: int = 0) -> List[ValidationResult]:
        """Validate a single record against all rules"""
        results = []
        
        for rule in self.rules:
            field_value = record.get(rule.field_name)
            
            try:
                validation_result = self._apply_rule(rule, field_value, record, record_index)
                if validation_result:
                    results.append(validation_result)
            except Exception as e:
                self.logger.error(f"Validation rule {rule.rule_type} failed for field {rule.field_name}: {str(e)}")
                results.append(ValidationResult(
                    field_name=rule.field_name,
                    rule_type=rule.rule_type,
                    severity=ValidationSeverity.ERROR,
                    message=f"Validation rule failed: {str(e)}",
                    value=field_value,
                    is_valid=False
                ))
        
        return results
    
    def validate_batch(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate a batch of records"""
        all_results = []
        valid_records = []
        invalid_records = []
        
        for i, record in enumerate(records):
            record_results = self.validate_record(record, i)
            
            # Check if record has any errors
            has_errors = any(result.severity == ValidationSeverity.ERROR for result in record_results)
            
            if has_errors:
                invalid_records.append({
                    'record_index': i,
                    'record': record,
                    'validation_results': record_results
                })
            else:
                valid_records.append(record)
            
            all_results.extend(record_results)
        
        # Generate summary statistics
        error_count = sum(1 for r in all_results if r.severity == ValidationSeverity.ERROR)
        warning_count = sum(1 for r in all_results if r.severity == ValidationSeverity.WARNING)
        
        return {
            'total_records': len(records),
            'valid_records': len(valid_records),
            'invalid_records': len(invalid_records),
            'validation_results': all_results,
            'valid_record_list': valid_records,
            'invalid_record_list': invalid_records,
            'error_count': error_count,
            'warning_count': warning_count,
            'validation_passed': error_count == 0
        }
    
    def _apply_rule(self, rule: ValidationRule, value: Any, record: Dict[str, Any], record_index: int) -> Optional[ValidationResult]:
        """Apply a specific validation rule"""
        
        # Handle custom validators
        if rule.custom_validator:
            try:
                is_valid, suggested_value = rule.custom_validator(value, record)
                if not is_valid:
                    return ValidationResult(
                        field_name=rule.field_name,
                        rule_type=rule.rule_type,
                        severity=rule.severity,
                        message=rule.message.format(value=value, record_index=record_index),
                        value=value,
                        is_valid=False,
                        suggested_value=suggested_value
                    )
            except Exception as e:
                return ValidationResult(
                    field_name=rule.field_name,
                    rule_type=rule.rule_type,
                    severity=ValidationSeverity.ERROR,
                    message=f"Custom validator error: {str(e)}",
                    value=value,
                    is_valid=False
                )
            return None
        
        # Handle built-in validators
        validator_name = rule.rule_type
        if validator_name in self.custom_validators:
            validator_func = self.custom_validators[validator_name]
            try:
                is_valid, suggested_value = validator_func(value, rule.parameters)
                if not is_valid:
                    return ValidationResult(
                        field_name=rule.field_name,
                        rule_type=rule.rule_type,
                        severity=rule.severity,
                        message=rule.message.format(value=value, record_index=record_index, **rule.parameters),
                        value=value,
                        is_valid=False,
                        suggested_value=suggested_value
                    )
            except Exception as e:
                return ValidationResult(
                    field_name=rule.field_name,
                    rule_type=rule.rule_type,
                    severity=ValidationSeverity.ERROR,
                    message=f"Validator {validator_name} error: {str(e)}",
                    value=value,
                    is_valid=False
                )
        
        return None
    
    def _register_builtin_validators(self):
        """Register built-in validation functions"""
        
        def validate_required(value, params):
            """Check if required field is present and not empty"""
            is_valid = value is not None and str(value).strip() != ""
            return is_valid, value
        
        def validate_type(value, params):
            """Check if value is of expected type"""
            expected_type = params.get('type', str)
            
            if value is None:
                return True, value
            
            if expected_type == 'int':
                converted = safe_int_convert(value)
                return converted is not None, converted
            elif expected_type == 'float':
                converted = safe_float_convert(value)
                return converted is not None, converted
            elif expected_type == 'date':
                parsed = parse_date(str(value)) if value else None
                return parsed is not None, parsed
            elif expected_type == 'string':
                return isinstance(value, str), str(value) if value else None
            
            return isinstance(value, expected_type), value
        
        def validate_range(value, params):
            """Check if numeric value is within specified range"""
            if value is None:
                return True, value
            
            numeric_value = safe_float_convert(value)
            if numeric_value is None:
                return False, value
            
            min_val = params.get('min')
            max_val = params.get('max')
            
            if min_val is not None and numeric_value < min_val:
                return False, min_val
            if max_val is not None and numeric_value > max_val:
                return False, max_val
            
            return True, numeric_value
        
        def validate_length(value, params):
            """Check if string length is within specified range"""
            if value is None:
                return True, value
            
            str_value = str(value)
            length = len(str_value)
            
            min_length = params.get('min', 0)
            max_length = params.get('max')
            
            if length < min_length:
                return False, value
            if max_length is not None and length > max_length:
                return False, str_value[:max_length]
            
            return True, str_value
        
        def validate_pattern(value, params):
            """Check if value matches regex pattern"""
            if value is None:
                return True, value
            
            pattern = params.get('pattern', '')
            flags = params.get('flags', 0)
            
            if not pattern:
                return True, value
            
            str_value = str(value)
            match = re.match(pattern, str_value, flags)
            
            return match is not None, str_value
        
        def validate_choices(value, params):
            """Check if value is in allowed choices"""
            if value is None:
                return True, value
            
            choices = params.get('choices', [])
            case_sensitive = params.get('case_sensitive', True)
            
            if not choices:
                return True, value
            
            if case_sensitive:
                return value in choices, value
            else:
                str_value = str(value).lower()
                lower_choices = [str(choice).lower() for choice in choices]
                return str_value in lower_choices, value
        
        def validate_email_format(value, params):
            """Check if value is a valid email address"""
            if value is None:
                return True, value
            
            return validate_email(str(value)), str(value).strip()
        
        def validate_phone_format(value, params):
            """Check if value is a valid phone number"""
            if value is None:
                return True, value
            
            return validate_phone(str(value)), str(value).strip()
        
        def validate_date_range(value, params):
            """Check if date is within specified range"""
            if value is None:
                return True, value
            
            parsed_date = parse_date(str(value))
            if parsed_date is None:
                return False, value
            
            min_date = params.get('min_date')
            max_date = params.get('max_date')
            
            if min_date:
                if isinstance(min_date, str):
                    min_date = parse_date(min_date)
                if parsed_date < min_date:
                    return False, min_date
            
            if max_date:
                if isinstance(max_date, str):
                    max_date = parse_date(max_date)
                if parsed_date > max_date:
                    return False, max_date
            
            return True, parsed_date
        
        def validate_positive(value, params):
            """Check if numeric value is positive"""
            if value is None:
                return True, value
            
            numeric_value = safe_float_convert(value)
            if numeric_value is None:
                return False, value
            
            return numeric_value > 0, numeric_value
        
        def validate_unique(value, params):
            """Check for uniqueness within the batch (requires context)"""
            # This would need to be implemented with batch context
            return True, value
        
        # Register all validators
        validators = {
            'required': validate_required,
            'type': validate_type,
            'range': validate_range,
            'length': validate_length,
            'pattern': validate_pattern,
            'choices': validate_choices,
            'email': validate_email_format,
            'phone': validate_phone_format,
            'date_range': validate_date_range,
            'positive': validate_positive,
            'unique': validate_unique
        }
        
        for name, func in validators.items():
            self.custom_validators[name] = func

class RouteDataValidator(DataValidator):
    """Specialized validator for route/trucking data"""
    
    def __init__(self):
        super().__init__("route_data")
        self._setup_route_validation_rules()
    
    def _setup_route_validation_rules(self):
        """Setup validation rules specific to route data"""
        
        # Route ID validation
        self.add_rule(ValidationRule(
            field_name='route_id',
            rule_type='required',
            parameters={},
            severity=ValidationSeverity.ERROR,
            message="Route ID is required"
        ))
        
        self.add_rule(ValidationRule(
            field_name='route_id',
            rule_type='length',
            parameters={'min': 1, 'max': 50},
            severity=ValidationSeverity.ERROR,
            message="Route ID must be 1-50 characters"
        ))
        
        # Date validation
        self.add_rule(ValidationRule(
            field_name='route_date',
            rule_type='required',
            parameters={},
            severity=ValidationSeverity.ERROR,
            message="Route date is required"
        ))
        
        self.add_rule(ValidationRule(
            field_name='route_date',
            rule_type='type',
            parameters={'type': 'date'},
            severity=ValidationSeverity.ERROR,
            message="Route date must be a valid date"
        ))
        
        # Mileage validation
        self.add_rule(ValidationRule(
            field_name='total_miles',
            rule_type='type',
            parameters={'type': 'float'},
            severity=ValidationSeverity.WARNING,
            message="Total miles should be numeric"
        ))
        
        self.add_rule(ValidationRule(
            field_name='total_miles',
            rule_type='positive',
            parameters={},
            severity=ValidationSeverity.WARNING,
            message="Total miles should be positive"
        ))
        
        self.add_rule(ValidationRule(
            field_name='total_miles',
            rule_type='range',
            parameters={'min': 0, 'max': 5000},
            severity=ValidationSeverity.WARNING,
            message="Total miles seems unusually high (over 5000 miles)"
        ))
        
        # Revenue validation
        self.add_rule(ValidationRule(
            field_name='revenue',
            rule_type='type',
            parameters={'type': 'float'},
            severity=ValidationSeverity.WARNING,
            message="Revenue should be numeric"
        ))
        
        self.add_rule(ValidationRule(
            field_name='revenue',
            rule_type='positive',
            parameters={},
            severity=ValidationSeverity.WARNING,
            message="Revenue should be positive"
        ))
        
        # Driver name validation
        self.add_rule(ValidationRule(
            field_name='driver_name',
            rule_type='length',
            parameters={'min': 2, 'max': 100},
            severity=ValidationSeverity.WARNING,
            message="Driver name should be 2-100 characters"
        ))
        
        # Email validation (if present)
        self.add_rule(ValidationRule(
            field_name='email',
            rule_type='email',
            parameters={},
            severity=ValidationSeverity.WARNING,
            message="Invalid email format"
        ))
        
        # Phone validation (if present)
        self.add_rule(ValidationRule(
            field_name='phone',
            rule_type='phone',
            parameters={},
            severity=ValidationSeverity.WARNING,
            message="Invalid phone number format"
        ))
        
        # Status validation
        self.add_rule(ValidationRule(
            field_name='status',
            rule_type='choices',
            parameters={
                'choices': ['scheduled', 'in_progress', 'completed', 'cancelled', 'delayed'],
                'case_sensitive': False
            },
            severity=ValidationSeverity.WARNING,
            message="Status should be one of: scheduled, in_progress, completed, cancelled, delayed"
        ))

def create_validator_from_config(config: Dict[str, Any]) -> DataValidator:
    """Create a validator from configuration"""
    validator_name = config.get('name', 'custom')
    validator = DataValidator(validator_name)
    
    # Add rules from config
    rules_config = config.get('rules', [])
    for rule_config in rules_config:
        rule = ValidationRule(
            field_name=rule_config['field_name'],
            rule_type=rule_config['rule_type'],
            parameters=rule_config.get('parameters', {}),
            severity=ValidationSeverity(rule_config.get('severity', 'warning')),
            message=rule_config['message']
        )
        validator.add_rule(rule)
    
    return validator