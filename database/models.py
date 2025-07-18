import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum

class LoadType(Enum):
    GENERAL = "general"
    REFRIGERATED = "refrigerated"
    HAZARDOUS = "hazardous"
    OVERSIZED = "oversized"
    FRAGILE = "fragile"

class RouteStatus(Enum):
    SCHEDULED = "scheduled"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    DELAYED = "delayed"

@dataclass
class Location:
    id: Optional[int] = None
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

@dataclass
class Customer:
    id: Optional[int] = None
    name: str = ""
    contact_person: str = ""
    phone: str = ""
    email: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    payment_terms: str = ""
    rating: Optional[float] = None
    notes: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

@dataclass
class Driver:
    id: Optional[int] = None
    name: str = ""
    license_number: str = ""
    phone: str = ""
    email: str = ""
    hire_date: Optional[datetime] = None
    hourly_rate: Optional[float] = None
    performance_rating: Optional[float] = None
    total_miles: int = 0
    total_routes: int = 0
    safety_score: Optional[float] = None
    active: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

@dataclass
class Vehicle:
    id: Optional[int] = None
    make: str = ""
    model: str = ""
    year: int = 0
    license_plate: str = ""
    vin: str = ""
    capacity_weight: Optional[float] = None
    capacity_volume: Optional[float] = None
    fuel_type: str = ""
    mpg_average: Optional[float] = None
    last_maintenance: Optional[datetime] = None
    next_maintenance: Optional[datetime] = None
    active: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

@dataclass
class Route:
    id: Optional[int] = None
    route_date: datetime = field(default_factory=datetime.now)
    driver_id: Optional[int] = None
    vehicle_id: Optional[int] = None
    customer_id: Optional[int] = None
    
    start_location_id: Optional[int] = None
    end_location_id: Optional[int] = None
    
    load_weight: Optional[float] = None
    load_type: LoadType = LoadType.GENERAL
    load_value: Optional[float] = None
    special_requirements: str = ""
    
    scheduled_start_time: Optional[datetime] = None
    actual_start_time: Optional[datetime] = None
    scheduled_end_time: Optional[datetime] = None
    actual_end_time: Optional[datetime] = None
    
    total_miles: Optional[float] = None
    empty_miles: Optional[float] = None
    fuel_consumed: Optional[float] = None
    average_speed: Optional[float] = None
    
    revenue: Optional[float] = None
    fuel_cost: Optional[float] = None
    toll_cost: Optional[float] = None
    driver_pay: Optional[float] = None
    other_costs: Optional[float] = None
    
    status: RouteStatus = RouteStatus.SCHEDULED
    notes: str = ""
    
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

@dataclass
class FinancialRecord:
    id: Optional[int] = None
    route_id: int = 0
    record_type: str = ""  # 'revenue', 'cost', 'expense'
    category: str = ""     # 'fuel', 'toll', 'maintenance', 'driver_pay', etc.
    amount: float = 0.0
    description: str = ""
    transaction_date: datetime = field(default_factory=datetime.now)
    created_at: datetime = field(default_factory=datetime.now)

class DatabaseSchema:
    @staticmethod
    def create_tables(conn: sqlite3.Connection):
        cursor = conn.cursor()
        
        # Locations table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT NOT NULL,
                zip_code TEXT,
                latitude REAL,
                longitude REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Customers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                contact_person TEXT,
                phone TEXT,
                email TEXT,
                address TEXT,
                city TEXT,
                state TEXT,
                zip_code TEXT,
                payment_terms TEXT,
                rating REAL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Drivers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS drivers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                license_number TEXT UNIQUE,
                phone TEXT,
                email TEXT,
                hire_date DATE,
                hourly_rate REAL,
                performance_rating REAL,
                total_miles INTEGER DEFAULT 0,
                total_routes INTEGER DEFAULT 0,
                safety_score REAL,
                active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Vehicles table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vehicles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                make TEXT NOT NULL,
                model TEXT NOT NULL,
                year INTEGER,
                license_plate TEXT UNIQUE,
                vin TEXT UNIQUE,
                capacity_weight REAL,
                capacity_volume REAL,
                fuel_type TEXT,
                mpg_average REAL,
                last_maintenance DATE,
                next_maintenance DATE,
                active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Routes table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS routes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                route_date DATE NOT NULL,
                driver_id INTEGER,
                vehicle_id INTEGER,
                customer_id INTEGER,
                
                start_location_id INTEGER,
                end_location_id INTEGER,
                
                load_weight REAL,
                load_type TEXT,
                load_value REAL,
                special_requirements TEXT,
                
                scheduled_start_time TIMESTAMP,
                actual_start_time TIMESTAMP,
                scheduled_end_time TIMESTAMP,
                actual_end_time TIMESTAMP,
                
                total_miles REAL,
                empty_miles REAL,
                fuel_consumed REAL,
                average_speed REAL,
                
                revenue REAL,
                fuel_cost REAL,
                toll_cost REAL,
                driver_pay REAL,
                other_costs REAL,
                
                status TEXT DEFAULT 'scheduled',
                notes TEXT,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (driver_id) REFERENCES drivers (id),
                FOREIGN KEY (vehicle_id) REFERENCES vehicles (id),
                FOREIGN KEY (customer_id) REFERENCES customers (id),
                FOREIGN KEY (start_location_id) REFERENCES locations (id),
                FOREIGN KEY (end_location_id) REFERENCES locations (id)
            )
        ''')
        
        # Financial records table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS financial_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                route_id INTEGER NOT NULL,
                record_type TEXT NOT NULL,
                category TEXT NOT NULL,
                amount REAL NOT NULL,
                description TEXT,
                transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (route_id) REFERENCES routes (id)
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_routes_date ON routes(route_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_routes_driver ON routes(driver_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_routes_vehicle ON routes(vehicle_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_routes_customer ON routes(customer_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_routes_status ON routes(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_financial_route ON financial_records(route_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_financial_type ON financial_records(record_type)')
        
        conn.commit()

    @staticmethod
    def create_triggers(conn: sqlite3.Connection):
        cursor = conn.cursor()
        
        # Trigger to update updated_at timestamp
        for table in ['locations', 'customers', 'drivers', 'vehicles', 'routes']:
            cursor.execute(f'''
                CREATE TRIGGER IF NOT EXISTS update_{table}_timestamp 
                AFTER UPDATE ON {table}
                BEGIN
                    UPDATE {table} SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                END
            ''')
        
        # Trigger to update driver statistics when route is completed
        cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS update_driver_stats
            AFTER UPDATE ON routes
            WHEN NEW.status = 'completed' AND OLD.status != 'completed'
            BEGIN
                UPDATE drivers SET 
                    total_miles = total_miles + COALESCE(NEW.total_miles, 0),
                    total_routes = total_routes + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = NEW.driver_id;
            END
        ''')
        
        conn.commit()