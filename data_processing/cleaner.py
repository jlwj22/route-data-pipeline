import pandas as pd
import re
from datetime import datetime
from typing import Dict, Any, Optional, List
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger

logger = get_logger(__name__)

class DataCleaner:
    """
    Data cleaning utilities for route pipeline data.
    Handles missing values, format standardization, and data validation.
    """
    
    def __init__(self):
        self.us_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
        }
        
        self.phone_pattern = re.compile(r'^(\+?1)?[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})$')
        self.email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        self.zip_pattern = re.compile(r'^\d{5}(-\d{4})?$')
    
    def clean_phone_number(self, phone: str) -> Optional[str]:
        """
        Clean and standardize phone number format.
        Returns phone in format: (XXX) XXX-XXXX
        """
        if not phone or pd.isna(phone):
            return None
            
        # Remove all non-digit characters
        digits = re.sub(r'\D', '', str(phone))
        
        # Handle different formats
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        else:
            logger.warning(f"Invalid phone number format: {phone}")
            return None
    
    def clean_email(self, email: str) -> Optional[str]:
        """
        Clean and validate email format.
        """
        if not email or pd.isna(email):
            return None
            
        email = str(email).strip().lower()
        
        if self.email_pattern.match(email):
            return email
        else:
            logger.warning(f"Invalid email format: {email}")
            return None
    
    def clean_zip_code(self, zip_code: str) -> Optional[str]:
        """
        Clean and validate ZIP code format.
        """
        if not zip_code or pd.isna(zip_code):
            return None
            
        # Remove spaces and standardize
        zip_code = str(zip_code).strip().replace(' ', '')
        
        if self.zip_pattern.match(zip_code):
            return zip_code
        else:
            logger.warning(f"Invalid ZIP code format: {zip_code}")
            return None
    
    def clean_state(self, state: str) -> Optional[str]:
        """
        Clean and validate state abbreviation.
        """
        if not state or pd.isna(state):
            return None
            
        state = str(state).strip().upper()
        
        if state in self.us_states:
            return state
        else:
            # Try to convert full state name to abbreviation
            state_mapping = {
                'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
                'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
                'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
                'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
                'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
                'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
                'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
                'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
                'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
                'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
                'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
                'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
                'WISCONSIN': 'WI', 'WYOMING': 'WY'
            }
            
            if state in state_mapping:
                return state_mapping[state]
            else:
                logger.warning(f"Invalid state: {state}")
                return None
    
    def clean_address(self, address: str) -> Optional[str]:
        """
        Clean and standardize address format.
        """
        if not address or pd.isna(address):
            return None
            
        address = str(address).strip()
        
        # Basic address cleaning
        address = re.sub(r'\s+', ' ', address)  # Multiple spaces to single space
        address = address.title()  # Title case
        
        # Common abbreviations
        abbreviations = {
            'Street': 'St', 'Avenue': 'Ave', 'Boulevard': 'Blvd', 'Drive': 'Dr',
            'Lane': 'Ln', 'Road': 'Rd', 'Circle': 'Cir', 'Court': 'Ct',
            'Place': 'Pl', 'Square': 'Sq', 'Trail': 'Trl', 'Parkway': 'Pkwy',
            'North': 'N', 'South': 'S', 'East': 'E', 'West': 'W',
            'Northeast': 'NE', 'Northwest': 'NW', 'Southeast': 'SE', 'Southwest': 'SW'
        }
        
        for full, abbr in abbreviations.items():
            address = re.sub(rf'\b{full}\b', abbr, address, flags=re.IGNORECASE)
        
        return address
    
    def clean_datetime(self, dt: Any) -> Optional[datetime]:
        """
        Clean and parse datetime values.
        """
        if pd.isna(dt):
            return None
            
        if isinstance(dt, datetime):
            return dt
            
        if isinstance(dt, str):
            # Try common datetime formats
            formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%Y-%m-%d',
                '%m/%d/%Y %H:%M:%S',
                '%m/%d/%Y %H:%M',
                '%m/%d/%Y',
                '%m-%d-%Y %H:%M:%S',
                '%m-%d-%Y %H:%M',
                '%m-%d-%Y',
                '%Y/%m/%d %H:%M:%S',
                '%Y/%m/%d %H:%M',
                '%Y/%m/%d'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(dt, fmt)
                except ValueError:
                    continue
            
            logger.warning(f"Unable to parse datetime: {dt}")
            return None
        
        return None
    
    def clean_numeric(self, value: Any) -> Optional[float]:
        """
        Clean and convert numeric values.
        """
        if pd.isna(value):
            return None
            
        if isinstance(value, (int, float)):
            return float(value)
            
        if isinstance(value, str):
            # Remove common non-numeric characters
            value = value.strip().replace(',', '').replace('$', '').replace('%', '')
            
            try:
                return float(value)
            except ValueError:
                logger.warning(f"Unable to convert to numeric: {value}")
                return None
        
        return None
    
    def clean_route_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean a complete route data record.
        """
        cleaned = {}
        
        # Basic fields
        cleaned['route_date'] = self.clean_datetime(data.get('route_date'))
        
        # Location fields
        for location_type in ['start_location', 'end_location']:
            if location_type in data:
                loc_data = data[location_type]
                cleaned[location_type] = {
                    'address': self.clean_address(loc_data.get('address')),
                    'city': loc_data.get('city', '').strip().title() if loc_data.get('city') else None,
                    'state': self.clean_state(loc_data.get('state')),
                    'zip_code': self.clean_zip_code(loc_data.get('zip_code'))
                }
        
        # Customer fields
        if 'customer' in data:
            cust_data = data['customer']
            cleaned['customer'] = {
                'name': cust_data.get('name', '').strip().title() if cust_data.get('name') else None,
                'phone': self.clean_phone_number(cust_data.get('phone')),
                'email': self.clean_email(cust_data.get('email'))
            }
        
        # Driver fields
        if 'driver' in data:
            driver_data = data['driver']
            cleaned['driver'] = {
                'name': driver_data.get('name', '').strip().title() if driver_data.get('name') else None,
                'phone': self.clean_phone_number(driver_data.get('phone')),
                'email': self.clean_email(driver_data.get('email'))
            }
        
        # Numeric fields
        numeric_fields = [
            'load_weight', 'load_value', 'total_miles', 'empty_miles',
            'fuel_consumed', 'average_speed', 'revenue', 'fuel_cost',
            'toll_cost', 'driver_pay', 'other_costs'
        ]
        
        for field in numeric_fields:
            if field in data:
                cleaned[field] = self.clean_numeric(data[field])
        
        # Datetime fields
        datetime_fields = [
            'scheduled_start_time', 'actual_start_time',
            'scheduled_end_time', 'actual_end_time'
        ]
        
        for field in datetime_fields:
            if field in data:
                cleaned[field] = self.clean_datetime(data[field])
        
        # Text fields
        text_fields = ['load_type', 'special_requirements', 'status', 'notes']
        for field in text_fields:
            if field in data:
                cleaned[field] = str(data[field]).strip() if data[field] else None
        
        return cleaned
    
    def remove_duplicates(self, data: List[Dict[str, Any]], 
                         key_fields: List[str] = None) -> List[Dict[str, Any]]:
        """
        Remove duplicate records based on key fields.
        """
        if not key_fields:
            key_fields = ['route_date', 'driver_id', 'start_location', 'end_location']
        
        seen = set()
        unique_data = []
        
        for record in data:
            # Create a tuple of key field values for comparison
            key_values = tuple(
                str(record.get(field, '')) for field in key_fields
            )
            
            if key_values not in seen:
                seen.add(key_values)
                unique_data.append(record)
            else:
                logger.warning(f"Duplicate record found and removed: {key_values}")
        
        logger.info(f"Removed {len(data) - len(unique_data)} duplicate records")
        return unique_data
    
    def validate_required_fields(self, data: Dict[str, Any], 
                                required_fields: List[str]) -> bool:
        """
        Validate that required fields are present and not empty.
        """
        missing_fields = []
        
        for field in required_fields:
            if field not in data or data[field] is None or data[field] == '':
                missing_fields.append(field)
        
        if missing_fields:
            logger.warning(f"Missing required fields: {missing_fields}")
            return False
        
        return True
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a pandas DataFrame containing route data.
        """
        logger.info(f"Cleaning DataFrame with {len(df)} records")
        
        # Make a copy to avoid modifying the original
        cleaned_df = df.copy()
        
        # Clean phone numbers
        phone_columns = [col for col in cleaned_df.columns if 'phone' in col.lower()]
        for col in phone_columns:
            cleaned_df[col] = cleaned_df[col].apply(self.clean_phone_number)
        
        # Clean email addresses
        email_columns = [col for col in cleaned_df.columns if 'email' in col.lower()]
        for col in email_columns:
            cleaned_df[col] = cleaned_df[col].apply(self.clean_email)
        
        # Clean ZIP codes
        zip_columns = [col for col in cleaned_df.columns if 'zip' in col.lower()]
        for col in zip_columns:
            cleaned_df[col] = cleaned_df[col].apply(self.clean_zip_code)
        
        # Clean state abbreviations
        state_columns = [col for col in cleaned_df.columns if 'state' in col.lower()]
        for col in state_columns:
            cleaned_df[col] = cleaned_df[col].apply(self.clean_state)
        
        # Clean addresses
        address_columns = [col for col in cleaned_df.columns if 'address' in col.lower()]
        for col in address_columns:
            cleaned_df[col] = cleaned_df[col].apply(self.clean_address)
        
        # Clean datetime columns
        datetime_columns = [col for col in cleaned_df.columns if 'time' in col.lower() or 'date' in col.lower()]
        for col in datetime_columns:
            cleaned_df[col] = cleaned_df[col].apply(self.clean_datetime)
        
        # Clean numeric columns
        numeric_columns = [
            col for col in cleaned_df.columns 
            if any(keyword in col.lower() for keyword in [
                'miles', 'weight', 'value', 'speed', 'cost', 'revenue', 'pay', 'fuel'
            ])
        ]
        for col in numeric_columns:
            cleaned_df[col] = cleaned_df[col].apply(self.clean_numeric)
        
        logger.info(f"Completed cleaning DataFrame")
        return cleaned_df