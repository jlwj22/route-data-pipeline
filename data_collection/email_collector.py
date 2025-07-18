import imaplib
import email
import smtplib
import re
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import base64
import quopri

from .base_collector import BaseCollector, CollectionResult, CollectionStatus
from utils.helpers import safe_float_convert, safe_int_convert, parse_date, clean_string

class EmailCollector(BaseCollector):
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        
        # Email server configuration
        self.imap_server = config.get('imap_server', '')
        self.imap_port = config.get('imap_port', 993)
        self.username = config.get('username', '')
        self.password = config.get('password', '')
        self.use_ssl = config.get('use_ssl', True)
        
        # Email filtering
        self.mailbox = config.get('mailbox', 'INBOX')
        self.sender_filters = config.get('sender_filters', [])
        self.subject_filters = config.get('subject_filters', [])
        self.days_back = config.get('days_back', 7)
        self.mark_as_read = config.get('mark_as_read', False)
        self.move_to_folder = config.get('move_to_folder', None)
        
        # Email parsing patterns
        self.parsing_patterns = config.get('parsing_patterns', {})
        self.text_extraction_rules = config.get('text_extraction_rules', [])
        
        # Attachment handling
        self.process_attachments = config.get('process_attachments', True)
        self.attachment_extensions = config.get('attachment_extensions', ['.csv', '.xlsx', '.pdf'])
        self.attachment_save_path = config.get('attachment_save_path', 'data/input/email_attachments')
        
        self.connection = None
    
    def validate_configuration(self) -> bool:
        """Validate email collector configuration"""
        if not self.imap_server:
            self.logger.error("IMAP server is required")
            return False
        
        if not self.username or not self.password:
            self.logger.error("Username and password are required")
            return False
        
        if not self.parsing_patterns and not self.text_extraction_rules:
            self.logger.warning("No parsing patterns or extraction rules defined")
        
        return True
    
    def test_connection(self) -> bool:
        """Test connection to email server"""
        if not self.validate_configuration():
            return False
        
        try:
            self._connect()
            self._disconnect()
            self.logger.info(f"Email connection test successful for {self.name}")
            return True
        except Exception as e:
            self.logger.error(f"Email connection test failed for {self.name}: {str(e)}")
            return False
    
    def _connect(self):
        """Connect to IMAP server"""
        if self.use_ssl:
            self.connection = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
        else:
            self.connection = imaplib.IMAP4(self.imap_server, self.imap_port)
        
        self.connection.login(self.username, self.password)
        self.connection.select(self.mailbox)
    
    def _disconnect(self):
        """Disconnect from IMAP server"""
        if self.connection:
            try:
                self.connection.close()
                self.connection.logout()
            except:
                pass
            self.connection = None
    
    def collect_data(self) -> CollectionResult:
        """Collect data from emails"""
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
        
        try:
            self._connect()
            
            # Search for emails
            email_ids = self._search_emails()
            
            if not email_ids:
                return CollectionResult(
                    status=CollectionStatus.NO_DATA,
                    data=[],
                    errors=[],
                    warnings=["No emails found matching criteria"],
                    metadata={'emails_searched': 0},
                    collected_at=datetime.now(),
                    source_info=self.get_source_info()
                )
            
            self.logger.info(f"Found {len(email_ids)} emails to process")
            
            processed_count = 0
            for email_id in email_ids:
                try:
                    email_data = self._process_email(email_id)
                    
                    if email_data['success']:
                        all_data.extend(email_data['data'])
                        processed_count += 1
                        
                        # Mark as read if configured
                        if self.mark_as_read:
                            self.connection.store(email_id, '+FLAGS', '\\Seen')
                        
                        # Move to folder if configured
                        if self.move_to_folder:
                            self._move_email_to_folder(email_id, self.move_to_folder)
                    else:
                        all_errors.extend(email_data['errors'])
                    
                    all_warnings.extend(email_data['warnings'])
                    
                except Exception as e:
                    error_msg = f"Failed to process email {email_id}: {str(e)}"
                    all_errors.append(error_msg)
                    self.logger.error(error_msg)
            
            metadata.update({
                'emails_found': len(email_ids),
                'emails_processed': processed_count,
                'emails_with_errors': len(email_ids) - processed_count
            })
            
        except Exception as e:
            all_errors.append(f"Email collection failed: {str(e)}")
            self.logger.error(f"Email collection failed: {str(e)}")
        finally:
            self._disconnect()
        
        return self.process_collected_data(all_data)
    
    def _search_emails(self) -> List[str]:
        """Search for emails based on filters"""
        search_criteria = []
        
        # Date filter
        since_date = (datetime.now() - timedelta(days=self.days_back)).strftime('%d-%b-%Y')
        search_criteria.append(f'SINCE {since_date}')
        
        # Sender filters
        if self.sender_filters:
            sender_criteria = []
            for sender in self.sender_filters:
                sender_criteria.append(f'FROM "{sender}"')
            search_criteria.append(f'({" OR ".join(sender_criteria)})')
        
        # Subject filters
        if self.subject_filters:
            subject_criteria = []
            for subject in self.subject_filters:
                subject_criteria.append(f'SUBJECT "{subject}"')
            search_criteria.append(f'({" OR ".join(subject_criteria)})')
        
        # Only unread emails if not marking as read
        if not self.mark_as_read:
            search_criteria.append('UNSEEN')
        
        search_string = ' '.join(search_criteria)
        self.logger.info(f"Email search criteria: {search_string}")
        
        typ, data = self.connection.search(None, search_string)
        
        if typ != 'OK':
            self.logger.error(f"Email search failed: {typ}")
            return []
        
        email_ids = data[0].split()
        return [email_id.decode() for email_id in email_ids]
    
    def _process_email(self, email_id: str) -> Dict[str, Any]:
        """Process a single email and extract route data"""
        try:
            # Fetch email
            typ, msg_data = self.connection.fetch(email_id, '(RFC822)')
            
            if typ != 'OK':
                return {
                    'success': False,
                    'data': [],
                    'errors': [f"Failed to fetch email {email_id}"],
                    'warnings': []
                }
            
            # Parse email
            email_message = email.message_from_bytes(msg_data[0][1])
            
            # Extract basic email info
            email_info = {
                'message_id': email_message.get('Message-ID', ''),
                'subject': email_message.get('Subject', ''),
                'sender': email_message.get('From', ''),
                'date': email_message.get('Date', ''),
                'email_id': email_id
            }
            
            extracted_data = []
            errors = []
            warnings = []
            
            # Extract text content
            text_content = self._extract_text_content(email_message)
            
            if text_content:
                # Parse route data from text
                text_data = self._parse_text_for_route_data(text_content, email_info)
                extracted_data.extend(text_data)
            
            # Process attachments if enabled
            if self.process_attachments:
                attachment_data = self._process_attachments(email_message, email_info)
                extracted_data.extend(attachment_data)
            
            if not extracted_data:
                warnings.append(f"No route data extracted from email {email_id}")
            
            return {
                'success': len(errors) == 0,
                'data': extracted_data,
                'errors': errors,
                'warnings': warnings
            }
            
        except Exception as e:
            return {
                'success': False,
                'data': [],
                'errors': [f"Error processing email {email_id}: {str(e)}"],
                'warnings': []
            }
    
    def _extract_text_content(self, email_message) -> str:
        """Extract text content from email"""
        text_content = ""
        
        if email_message.is_multipart():
            for part in email_message.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get('Content-Disposition', ''))
                
                # Skip attachments
                if 'attachment' in content_disposition:
                    continue
                
                if content_type == 'text/plain':
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        payload = part.get_payload(decode=True)
                        if payload:
                            text_content += payload.decode(charset, errors='ignore')
                    except:
                        text_content += str(part.get_payload())
                
                elif content_type == 'text/html':
                    # Basic HTML to text conversion
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        payload = part.get_payload(decode=True)
                        if payload:
                            html_content = payload.decode(charset, errors='ignore')
                            # Simple HTML tag removal
                            text_content += re.sub(r'<[^>]+>', '', html_content)
                    except:
                        pass
        else:
            # Single part email
            content_type = email_message.get_content_type()
            if content_type in ['text/plain', 'text/html']:
                charset = email_message.get_content_charset() or 'utf-8'
                try:
                    payload = email_message.get_payload(decode=True)
                    if payload:
                        content = payload.decode(charset, errors='ignore')
                        if content_type == 'text/html':
                            content = re.sub(r'<[^>]+>', '', content)
                        text_content = content
                except:
                    text_content = str(email_message.get_payload())
        
        return text_content.strip()
    
    def _parse_text_for_route_data(self, text_content: str, email_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse text content for route data using configured patterns"""
        extracted_data = []
        
        # Apply text extraction rules
        for rule in self.text_extraction_rules:
            rule_name = rule.get('name', 'unnamed_rule')
            patterns = rule.get('patterns', {})
            
            route_data = {'source': 'email_text', 'rule_used': rule_name}
            route_data.update(email_info)
            
            # Extract data using regex patterns
            for field_name, pattern in patterns.items():
                try:
                    match = re.search(pattern, text_content, re.IGNORECASE | re.MULTILINE)
                    if match:
                        route_data[field_name] = match.group(1).strip()
                except Exception as e:
                    self.logger.warning(f"Pattern matching failed for {field_name}: {str(e)}")
            
            # Only add if we extracted some meaningful data
            meaningful_fields = [k for k, v in route_data.items() 
                               if k not in ['source', 'rule_used', 'message_id', 'subject', 'sender', 'date', 'email_id'] 
                               and v]
            
            if meaningful_fields:
                extracted_data.append(route_data)
        
        # Apply parsing patterns (structured data extraction)
        for pattern_name, pattern_config in self.parsing_patterns.items():
            try:
                pattern_data = self._apply_parsing_pattern(text_content, pattern_config, email_info)
                extracted_data.extend(pattern_data)
            except Exception as e:
                self.logger.warning(f"Parsing pattern {pattern_name} failed: {str(e)}")
        
        return extracted_data
    
    def _apply_parsing_pattern(self, text: str, pattern_config: Dict[str, Any], email_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply a specific parsing pattern to extract structured data"""
        extracted_data = []
        
        # Table-like data extraction
        if pattern_config.get('type') == 'table':
            table_pattern = pattern_config.get('table_pattern', '')
            column_mapping = pattern_config.get('column_mapping', {})
            
            # Find table data
            table_matches = re.findall(table_pattern, text, re.MULTILINE)
            
            for match in table_matches:
                route_data = {'source': 'email_table'}
                route_data.update(email_info)
                
                if isinstance(match, tuple):
                    for i, value in enumerate(match):
                        column_name = column_mapping.get(str(i), f'column_{i}')
                        route_data[column_name] = clean_string(value)
                else:
                    route_data['extracted_text'] = clean_string(match)
                
                extracted_data.append(route_data)
        
        # Key-value pair extraction
        elif pattern_config.get('type') == 'key_value':
            kv_patterns = pattern_config.get('patterns', {})
            
            route_data = {'source': 'email_keyvalue'}
            route_data.update(email_info)
            
            for field_name, pattern in kv_patterns.items():
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    route_data[field_name] = clean_string(match.group(1))
            
            # Only add if we found some data
            if len(route_data) > len(email_info) + 1:
                extracted_data.append(route_data)
        
        return extracted_data
    
    def _process_attachments(self, email_message, email_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process email attachments for route data"""
        attachment_data = []
        
        for part in email_message.walk():
            content_disposition = str(part.get('Content-Disposition', ''))
            
            if 'attachment' in content_disposition:
                filename = part.get_filename()
                
                if filename:
                    # Check if attachment extension is allowed
                    file_ext = os.path.splitext(filename)[1].lower()
                    if file_ext not in self.attachment_extensions:
                        continue
                    
                    try:
                        # Save attachment
                        attachment_path = self._save_attachment(part, filename, email_info)
                        
                        # Create attachment record
                        attachment_record = {
                            'source': 'email_attachment',
                            'attachment_filename': filename,
                            'attachment_path': attachment_path,
                            'file_extension': file_ext
                        }
                        attachment_record.update(email_info)
                        
                        attachment_data.append(attachment_record)
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to process attachment {filename}: {str(e)}")
        
        return attachment_data
    
    def _save_attachment(self, part, filename: str, email_info: Dict[str, Any]) -> str:
        """Save email attachment to file system"""
        import os
        from utils.helpers import ensure_directory_exists
        
        ensure_directory_exists(self.attachment_save_path)
        
        # Create unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        email_id = email_info.get('email_id', 'unknown')
        safe_filename = re.sub(r'[^\w\-_\.]', '_', filename)
        
        attachment_filename = f"{timestamp}_{email_id}_{safe_filename}"
        attachment_path = os.path.join(self.attachment_save_path, attachment_filename)
        
        # Save file
        with open(attachment_path, 'wb') as f:
            f.write(part.get_payload(decode=True))
        
        self.logger.info(f"Saved attachment: {attachment_path}")
        return attachment_path
    
    def _move_email_to_folder(self, email_id: str, folder_name: str):
        """Move email to specified folder"""
        try:
            # Create folder if it doesn't exist
            self.connection.create(folder_name)
            
            # Move email
            self.connection.move(email_id, folder_name)
            self.logger.info(f"Moved email {email_id} to folder {folder_name}")
            
        except Exception as e:
            self.logger.warning(f"Failed to move email {email_id} to folder {folder_name}: {str(e)}")
    
    def get_required_fields(self) -> List[str]:
        """Get required fields for route data"""
        return ['subject', 'sender']
    
    def standardize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert email record to standard route data format"""
        standardized = {
            'source': 'email',
            'source_name': self.name,
            'collected_at': datetime.now().isoformat(),
            'raw_data': record.copy()
        }
        
        # Copy email metadata
        email_fields = ['message_id', 'subject', 'sender', 'date', 'email_id']
        for field in email_fields:
            if field in record:
                standardized[f'email_{field}'] = record[field]
        
        # Apply standard field mapping
        field_mapping = {
            'route_id': ['route_id', 'trip_id', 'load_id', 'job_id'],
            'route_date': ['date', 'trip_date', 'pickup_date', 'dispatch_date'],
            'driver_name': ['driver', 'driver_name'],
            'vehicle_id': ['truck', 'vehicle', 'vehicle_id', 'truck_number'],
            'customer_name': ['customer', 'shipper', 'consignee'],
            'origin_address': ['pickup', 'origin', 'from_address'],
            'destination_address': ['delivery', 'destination', 'to_address'],
            'total_miles': ['miles', 'distance', 'total_miles'],
            'revenue': ['revenue', 'rate', 'amount', 'pay'],
            'load_weight': ['weight', 'cargo_weight'],
            'status': ['status', 'trip_status']
        }
        
        for standard_field, possible_fields in field_mapping.items():
            value = None
            for field in possible_fields:
                if field in record and record[field] is not None:
                    value = record[field]
                    break
            
            if value is not None:
                # Apply type conversions
                if standard_field in ['total_miles', 'revenue', 'load_weight']:
                    standardized[standard_field] = safe_float_convert(value)
                elif standard_field == 'route_date':
                    parsed_date = parse_date(str(value))
                    standardized[standard_field] = parsed_date.isoformat() if parsed_date else None
                elif isinstance(value, str):
                    standardized[standard_field] = clean_string(value)
                else:
                    standardized[standard_field] = value
        
        return standardized