# Route Data Pipeline System

A comprehensive Python-based data pipeline system for processing trucking/logistics route data and generating automated spreadsheet reports.

## Overview

This system automates the collection, processing, and reporting of trucking route data while providing direct control over data processing and maintaining data integrity. It's designed to reduce manual data processing time by 80%+ while ensuring accuracy and consistency.

## Features

- **Automated Data Collection**: Support for multiple input sources (APIs, CSV files, email parsing, manual entry)
- **Data Processing**: Clean, validate, and transform raw data into structured format
- **Report Generation**: Create detailed Excel spreadsheets with charts and formatting
- **Scheduling**: Automated data collection and report generation
- **Database Management**: SQLite-based storage with comprehensive data models
- **Configuration Management**: Centralized settings with environment-specific configurations
- **Logging & Monitoring**: Comprehensive logging and error tracking

## Project Structure

```
route_pipeline/
├── main.py                 # Main execution script
├── config/
│   ├── settings.py         # Configuration management
│   ├── config.ini          # User-configurable settings
│   └── collectors_example.json # Example collector configuration
├── data_collection/        # Data collection modules ✅ COMPLETED
│   ├── base_collector.py   # Base collector interface
│   ├── api_collector.py    # API data collection
│   ├── file_collector.py   # File-based data collection
│   ├── email_collector.py  # Email parsing
│   ├── manual_entry.py     # Manual data entry
│   ├── validator.py        # Data validation framework
│   └── collection_manager.py # Collection orchestration
├── data_processing/        # Data processing modules (Phase 3)
├── database/
│   ├── models.py           # Database schema definitions
│   └── operations.py       # Database CRUD operations
├── reporting/              # Report generation modules (Phase 4)
├── automation/             # Scheduling and automation (Phase 5)
├── utils/
│   ├── logger.py           # Logging configuration
│   └── helpers.py          # Utility functions
├── tests/                  # Unit tests
├── requirements.txt        # Python dependencies
└── data/
    ├── input/             # Input data files
    ├── output/            # Generated reports
    └── archive/           # Historical data backup
```

## Installation

1. **Clone or download the project**
2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Initialize the database**:
   ```bash
   python main.py setup
   ```

## Configuration

The system uses a configuration file at `config/config.ini` for basic settings and supports JSON configuration files for data collectors.

### Basic Configuration (`config.ini`)

Key settings include:
- **Database**: Location and backup settings
- **Logging**: Log levels and file rotation
- **API**: Timeout and retry settings
- **Scheduling**: Automated task timing
- **Email**: SMTP configuration for notifications
- **Reports**: Output formats and locations

### Data Collector Configuration

See `config/collectors_example.json` for a complete example of how to configure data collectors. The system supports:

- **File Collectors**: Process CSV/Excel files from designated directories
- **API Collectors**: Connect to trucking software APIs (TMS, ELD, Dispatch systems)
- **Email Collectors**: Parse route information from email notifications
- **Manual Entry**: Process manually entered data via JSON files

## Usage

### Command Line Interface

```bash
# Initialize database
python main.py setup

# Show system status
python main.py status

# List available collectors
python main.py collect --list

# Collect data from all sources
python main.py collect

# Collect data from specific source
python main.py collect --source file_import

# Process collected data
python main.py process

# Generate all reports
python main.py report

# Generate specific report type
python main.py report --type daily

# Backup database
python main.py backup

# Vacuum database
python main.py vacuum
```

### Data Collection Sources

#### 1. File Collection
Place CSV or Excel files in the `data/input` directory. The system will:
- Automatically detect and process files
- Validate data format and content
- Move processed files to archive
- Handle errors gracefully

#### 2. Manual Entry
Create a JSON file at `data/input/manual_entry.json` with route data:
```json
{
  "routes": [
    {
      "route_id": "RT001",
      "route_date": "2024-01-15",
      "driver_name": "John Doe",
      "vehicle_id": "TRUCK001",
      "total_miles": 250.5,
      "revenue": 1500.00
    }
  ]
}
```

#### 3. API Integration
Configure API collectors to connect to:
- Transportation Management Systems (TMS)
- Electronic Logging Devices (ELD)
- Dispatch systems
- Load boards

#### 4. Email Parsing
Set up email collectors to parse route information from:
- Dispatch notifications
- Load confirmations
- Route assignments
- Delivery confirmations

### Data Models

The system includes comprehensive data models for:

- **Routes**: Core route information with timing, distance, and financial data
- **Drivers**: Driver details and performance metrics
- **Vehicles**: Vehicle specifications and maintenance records
- **Customers**: Customer information and relationship data
- **Locations**: Standardized location data with geocoding
- **Financial Records**: Revenue and cost tracking

### Data Validation

Built-in validation framework ensures:
- Required fields are present
- Data types are correct
- Values are within expected ranges
- Business rules are enforced
- Duplicate detection

## Development Phases

### Phase 1: Foundation ✅ (Completed)
- [x] Project structure setup
- [x] Database schema and models
- [x] Configuration management
- [x] Logging system
- [x] Core utilities

### Phase 2: Data Collection ✅ (Completed)
- [x] API integration modules
- [x] File-based data collection
- [x] Email parsing functionality
- [x] Data validation framework
- [x] Manual entry interface
- [x] Error handling and retry logic
- [x] Collection manager and orchestration

### Phase 3: Data Processing (Next)
- [ ] Data cleaning functions
- [ ] Business calculations
- [ ] Geographic processing
- [ ] Data transformation pipeline

### Phase 4: Reporting
- [ ] Excel report generation
- [ ] Chart and visualization components
- [ ] Report templates
- [ ] Multi-format export

### Phase 5: Automation
- [ ] Task scheduling
- [ ] Email notification system
- [ ] Automated backup processes
- [ ] System monitoring

## Database Schema

The system uses SQLite with the following main tables:

- **routes**: Core route information
- **drivers**: Driver details and performance
- **vehicles**: Vehicle specifications and maintenance
- **customers**: Customer information
- **locations**: Standardized location data
- **financial_records**: Revenue and cost tracking

## Data Collection Features

### Robust Error Handling
- Automatic retry logic for failed collections
- Graceful degradation when sources are unavailable
- Detailed error logging and reporting
- Partial success handling

### Data Validation
- Configurable validation rules
- Field-level and record-level validation
- Data type checking and conversion
- Business rule enforcement

### Source Monitoring
- Connection testing for all sources
- Collection statistics and metrics
- Performance monitoring
- Status reporting

### Flexible Configuration
- JSON-based collector configuration
- Environment-specific settings
- Hot-reloading of configurations
- Extensible collector framework

## Performance & Scalability

- **Concurrent Processing**: Multiple collectors can run in parallel
- **Batch Processing**: Efficient handling of large datasets
- **Memory Management**: Streaming processing for large files
- **Rate Limiting**: Respectful API usage
- **Caching**: Intelligent caching of geocoding and validation results

## Security

- Secure storage of API keys and credentials
- Configuration-based credential management
- Access logging and audit trails
- Input validation and sanitization
- Email security (IMAP/SSL support)

## Requirements

- Python 3.8+
- SQLite3
- Internet connectivity (for API access and geocoding)
- Email server access (for notifications, optional)

## Dependencies

Key Python packages:
- `pandas`: Data manipulation and analysis
- `openpyxl`: Excel file creation
- `requests`: API interactions
- `schedule`: Task scheduling
- `geopy`: Geographic calculations
- `matplotlib`: Chart generation

## Troubleshooting

### Common Issues

1. **No collectors configured**: Create or update collector configuration
2. **API connection failures**: Check credentials and network connectivity
3. **File processing errors**: Verify file format and column mappings
4. **Email parsing issues**: Check IMAP settings and authentication

### Debugging

- Check log files in the `logs/` directory
- Use `python main.py status` to verify system state
- Test individual collectors with `python main.py collect --source <name>`
- Review configuration files for syntax errors

## Support

For issues or questions:
1. Check the log files in the `logs/` directory
2. Review the configuration settings
3. Verify database connectivity
4. Check system requirements

## Contributing

This system is designed to be extensible. Future phases will add:
- Advanced data processing capabilities
- Comprehensive reporting functionality
- Automation and scheduling features
- Additional data source connectors

## License

This project is developed for internal use in trucking/logistics operations.