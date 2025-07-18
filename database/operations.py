import sqlite3
import os
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from contextlib import contextmanager
from .models import *

class DatabaseOperations:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.ensure_database_exists()
        
    def ensure_database_exists(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with self.get_connection() as conn:
            DatabaseSchema.create_tables(conn)
            DatabaseSchema.create_triggers(conn)
    
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: tuple = ()) -> List[sqlite3.Row]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def execute_update(self, query: str, params: tuple = ()) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
    
    def execute_insert(self, query: str, params: tuple = ()) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.lastrowid

    # Location operations
    def create_location(self, location: Location) -> int:
        query = '''
            INSERT INTO locations (address, city, state, zip_code, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?)
        '''
        params = (location.address, location.city, location.state, 
                 location.zip_code, location.latitude, location.longitude)
        return self.execute_insert(query, params)
    
    def get_location(self, location_id: int) -> Optional[Location]:
        query = 'SELECT * FROM locations WHERE id = ?'
        rows = self.execute_query(query, (location_id,))
        if rows:
            row = rows[0]
            return Location(
                id=row['id'],
                address=row['address'],
                city=row['city'],
                state=row['state'],
                zip_code=row['zip_code'],
                latitude=row['latitude'],
                longitude=row['longitude'],
                created_at=datetime.fromisoformat(row['created_at']),
                updated_at=datetime.fromisoformat(row['updated_at'])
            )
        return None
    
    def find_or_create_location(self, address: str, city: str, state: str, zip_code: str = "") -> int:
        query = '''
            SELECT id FROM locations 
            WHERE address = ? AND city = ? AND state = ? AND zip_code = ?
        '''
        rows = self.execute_query(query, (address, city, state, zip_code))
        if rows:
            return rows[0]['id']
        
        location = Location(address=address, city=city, state=state, zip_code=zip_code)
        return self.create_location(location)

    # Customer operations
    def create_customer(self, customer: Customer) -> int:
        query = '''
            INSERT INTO customers (name, contact_person, phone, email, address, city, state, 
                                 zip_code, payment_terms, rating, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        params = (customer.name, customer.contact_person, customer.phone, customer.email,
                 customer.address, customer.city, customer.state, customer.zip_code,
                 customer.payment_terms, customer.rating, customer.notes)
        return self.execute_insert(query, params)
    
    def get_customer(self, customer_id: int) -> Optional[Customer]:
        query = 'SELECT * FROM customers WHERE id = ?'
        rows = self.execute_query(query, (customer_id,))
        if rows:
            row = rows[0]
            return Customer(
                id=row['id'],
                name=row['name'],
                contact_person=row['contact_person'],
                phone=row['phone'],
                email=row['email'],
                address=row['address'],
                city=row['city'],
                state=row['state'],
                zip_code=row['zip_code'],
                payment_terms=row['payment_terms'],
                rating=row['rating'],
                notes=row['notes'],
                created_at=datetime.fromisoformat(row['created_at']),
                updated_at=datetime.fromisoformat(row['updated_at'])
            )
        return None
    
    def get_all_customers(self) -> List[Customer]:
        query = 'SELECT * FROM customers ORDER BY name'
        rows = self.execute_query(query)
        customers = []
        for row in rows:
            customers.append(Customer(
                id=row['id'],
                name=row['name'],
                contact_person=row['contact_person'],
                phone=row['phone'],
                email=row['email'],
                address=row['address'],
                city=row['city'],
                state=row['state'],
                zip_code=row['zip_code'],
                payment_terms=row['payment_terms'],
                rating=row['rating'],
                notes=row['notes'],
                created_at=datetime.fromisoformat(row['created_at']),
                updated_at=datetime.fromisoformat(row['updated_at'])
            ))
        return customers

    # Driver operations
    def create_driver(self, driver: Driver) -> int:
        query = '''
            INSERT INTO drivers (name, license_number, phone, email, hire_date, hourly_rate,
                               performance_rating, safety_score, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        params = (driver.name, driver.license_number, driver.phone, driver.email,
                 driver.hire_date, driver.hourly_rate, driver.performance_rating,
                 driver.safety_score, driver.active)
        return self.execute_insert(query, params)
    
    def get_driver(self, driver_id: int) -> Optional[Driver]:
        query = 'SELECT * FROM drivers WHERE id = ?'
        rows = self.execute_query(query, (driver_id,))
        if rows:
            row = rows[0]
            return Driver(
                id=row['id'],
                name=row['name'],
                license_number=row['license_number'],
                phone=row['phone'],
                email=row['email'],
                hire_date=datetime.fromisoformat(row['hire_date']) if row['hire_date'] else None,
                hourly_rate=row['hourly_rate'],
                performance_rating=row['performance_rating'],
                total_miles=row['total_miles'],
                total_routes=row['total_routes'],
                safety_score=row['safety_score'],
                active=bool(row['active']),
                created_at=datetime.fromisoformat(row['created_at']),
                updated_at=datetime.fromisoformat(row['updated_at'])
            )
        return None
    
    def get_active_drivers(self) -> List[Driver]:
        query = 'SELECT * FROM drivers WHERE active = 1 ORDER BY name'
        rows = self.execute_query(query)
        drivers = []
        for row in rows:
            drivers.append(Driver(
                id=row['id'],
                name=row['name'],
                license_number=row['license_number'],
                phone=row['phone'],
                email=row['email'],
                hire_date=datetime.fromisoformat(row['hire_date']) if row['hire_date'] else None,
                hourly_rate=row['hourly_rate'],
                performance_rating=row['performance_rating'],
                total_miles=row['total_miles'],
                total_routes=row['total_routes'],
                safety_score=row['safety_score'],
                active=bool(row['active']),
                created_at=datetime.fromisoformat(row['created_at']),
                updated_at=datetime.fromisoformat(row['updated_at'])
            ))
        return drivers

    # Vehicle operations
    def create_vehicle(self, vehicle: Vehicle) -> int:
        query = '''
            INSERT INTO vehicles (make, model, year, license_plate, vin, capacity_weight,
                                capacity_volume, fuel_type, mpg_average, last_maintenance,
                                next_maintenance, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        params = (vehicle.make, vehicle.model, vehicle.year, vehicle.license_plate,
                 vehicle.vin, vehicle.capacity_weight, vehicle.capacity_volume,
                 vehicle.fuel_type, vehicle.mpg_average, vehicle.last_maintenance,
                 vehicle.next_maintenance, vehicle.active)
        return self.execute_insert(query, params)
    
    def get_vehicle(self, vehicle_id: int) -> Optional[Vehicle]:
        query = 'SELECT * FROM vehicles WHERE id = ?'
        rows = self.execute_query(query, (vehicle_id,))
        if rows:
            row = rows[0]
            return Vehicle(
                id=row['id'],
                make=row['make'],
                model=row['model'],
                year=row['year'],
                license_plate=row['license_plate'],
                vin=row['vin'],
                capacity_weight=row['capacity_weight'],
                capacity_volume=row['capacity_volume'],
                fuel_type=row['fuel_type'],
                mpg_average=row['mpg_average'],
                last_maintenance=datetime.fromisoformat(row['last_maintenance']) if row['last_maintenance'] else None,
                next_maintenance=datetime.fromisoformat(row['next_maintenance']) if row['next_maintenance'] else None,
                active=bool(row['active']),
                created_at=datetime.fromisoformat(row['created_at']),
                updated_at=datetime.fromisoformat(row['updated_at'])
            )
        return None
    
    def get_active_vehicles(self) -> List[Vehicle]:
        query = 'SELECT * FROM vehicles WHERE active = 1 ORDER BY make, model'
        rows = self.execute_query(query)
        vehicles = []
        for row in rows:
            vehicles.append(Vehicle(
                id=row['id'],
                make=row['make'],
                model=row['model'],
                year=row['year'],
                license_plate=row['license_plate'],
                vin=row['vin'],
                capacity_weight=row['capacity_weight'],
                capacity_volume=row['capacity_volume'],
                fuel_type=row['fuel_type'],
                mpg_average=row['mpg_average'],
                last_maintenance=datetime.fromisoformat(row['last_maintenance']) if row['last_maintenance'] else None,
                next_maintenance=datetime.fromisoformat(row['next_maintenance']) if row['next_maintenance'] else None,
                active=bool(row['active']),
                created_at=datetime.fromisoformat(row['created_at']),
                updated_at=datetime.fromisoformat(row['updated_at'])
            ))
        return vehicles

    # Route operations
    def create_route(self, route: Route) -> int:
        query = '''
            INSERT INTO routes (route_date, driver_id, vehicle_id, customer_id,
                              start_location_id, end_location_id, load_weight, load_type,
                              load_value, special_requirements, scheduled_start_time,
                              actual_start_time, scheduled_end_time, actual_end_time,
                              total_miles, empty_miles, fuel_consumed, average_speed,
                              revenue, fuel_cost, toll_cost, driver_pay, other_costs,
                              status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        params = (
            route.route_date, route.driver_id, route.vehicle_id, route.customer_id,
            route.start_location_id, route.end_location_id, route.load_weight,
            route.load_type.value if route.load_type else None, route.load_value,
            route.special_requirements, route.scheduled_start_time,
            route.actual_start_time, route.scheduled_end_time, route.actual_end_time,
            route.total_miles, route.empty_miles, route.fuel_consumed, route.average_speed,
            route.revenue, route.fuel_cost, route.toll_cost, route.driver_pay,
            route.other_costs, route.status.value if route.status else None, route.notes
        )
        return self.execute_insert(query, params)
    
    def get_route(self, route_id: int) -> Optional[Route]:
        query = 'SELECT * FROM routes WHERE id = ?'
        rows = self.execute_query(query, (route_id,))
        if rows:
            row = rows[0]
            return self._row_to_route(row)
        return None
    
    def get_routes_by_date_range(self, start_date: datetime, end_date: datetime) -> List[Route]:
        query = 'SELECT * FROM routes WHERE route_date BETWEEN ? AND ? ORDER BY route_date'
        rows = self.execute_query(query, (start_date.date(), end_date.date()))
        return [self._row_to_route(row) for row in rows]
    
    def get_routes_by_driver(self, driver_id: int, start_date: datetime = None, end_date: datetime = None) -> List[Route]:
        if start_date and end_date:
            query = 'SELECT * FROM routes WHERE driver_id = ? AND route_date BETWEEN ? AND ? ORDER BY route_date'
            rows = self.execute_query(query, (driver_id, start_date.date(), end_date.date()))
        else:
            query = 'SELECT * FROM routes WHERE driver_id = ? ORDER BY route_date'
            rows = self.execute_query(query, (driver_id,))
        return [self._row_to_route(row) for row in rows]
    
    def update_route_status(self, route_id: int, status: RouteStatus) -> bool:
        query = 'UPDATE routes SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?'
        rows_affected = self.execute_update(query, (status.value, route_id))
        return rows_affected > 0
    
    def _row_to_route(self, row: sqlite3.Row) -> Route:
        return Route(
            id=row['id'],
            route_date=datetime.fromisoformat(row['route_date']),
            driver_id=row['driver_id'],
            vehicle_id=row['vehicle_id'],
            customer_id=row['customer_id'],
            start_location_id=row['start_location_id'],
            end_location_id=row['end_location_id'],
            load_weight=row['load_weight'],
            load_type=LoadType(row['load_type']) if row['load_type'] else LoadType.GENERAL,
            load_value=row['load_value'],
            special_requirements=row['special_requirements'],
            scheduled_start_time=datetime.fromisoformat(row['scheduled_start_time']) if row['scheduled_start_time'] else None,
            actual_start_time=datetime.fromisoformat(row['actual_start_time']) if row['actual_start_time'] else None,
            scheduled_end_time=datetime.fromisoformat(row['scheduled_end_time']) if row['scheduled_end_time'] else None,
            actual_end_time=datetime.fromisoformat(row['actual_end_time']) if row['actual_end_time'] else None,
            total_miles=row['total_miles'],
            empty_miles=row['empty_miles'],
            fuel_consumed=row['fuel_consumed'],
            average_speed=row['average_speed'],
            revenue=row['revenue'],
            fuel_cost=row['fuel_cost'],
            toll_cost=row['toll_cost'],
            driver_pay=row['driver_pay'],
            other_costs=row['other_costs'],
            status=RouteStatus(row['status']) if row['status'] else RouteStatus.SCHEDULED,
            notes=row['notes'],
            created_at=datetime.fromisoformat(row['created_at']),
            updated_at=datetime.fromisoformat(row['updated_at'])
        )

    # Financial record operations
    def create_financial_record(self, record: FinancialRecord) -> int:
        query = '''
            INSERT INTO financial_records (route_id, record_type, category, amount, description, transaction_date)
            VALUES (?, ?, ?, ?, ?, ?)
        '''
        params = (record.route_id, record.record_type, record.category, record.amount, 
                 record.description, record.transaction_date)
        return self.execute_insert(query, params)
    
    def get_financial_records_by_route(self, route_id: int) -> List[FinancialRecord]:
        query = 'SELECT * FROM financial_records WHERE route_id = ? ORDER BY transaction_date'
        rows = self.execute_query(query, (route_id,))
        records = []
        for row in rows:
            records.append(FinancialRecord(
                id=row['id'],
                route_id=row['route_id'],
                record_type=row['record_type'],
                category=row['category'],
                amount=row['amount'],
                description=row['description'],
                transaction_date=datetime.fromisoformat(row['transaction_date']),
                created_at=datetime.fromisoformat(row['created_at'])
            ))
        return records

    # Utility methods
    def get_database_stats(self) -> Dict[str, int]:
        stats = {}
        tables = ['routes', 'drivers', 'vehicles', 'customers', 'locations', 'financial_records']
        
        for table in tables:
            query = f'SELECT COUNT(*) as count FROM {table}'
            result = self.execute_query(query)
            stats[table] = result[0]['count']
        
        return stats
    
    def backup_database(self, backup_path: str):
        import shutil
        shutil.copy2(self.db_path, backup_path)
    
    def vacuum_database(self):
        with self.get_connection() as conn:
            conn.execute('VACUUM')
            conn.commit()