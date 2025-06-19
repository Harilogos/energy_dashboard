"""
Script to create the new database tables according to the updated schema
"""
from sqlalchemy import text
from db_setup import engine, session

def create_tables():
    """Create all the new tables with the updated schema"""
    
    # Drop existing tables if they exist (be careful with this in production!)
    drop_tables = [
        "DROP TABLE IF EXISTS settlement_data",
        "DROP TABLE IF EXISTS consumption_mapping", 
        "DROP TABLE IF EXISTS tbl_consumption",
        "DROP TABLE IF EXISTS tbl_generation",
        "DROP TABLE IF EXISTS tbl_plants",
        "DROP TABLE IF EXISTS generation_data",
        "DROP TABLE IF EXISTS consumption_data",
        "DROP TABLE IF EXISTS consumption_percentage_data"
    ]
    
    # Create new tables
    create_tables = [
        """CREATE TABLE IF NOT EXISTS tbl_plants (
            plant_id VARCHAR(50) PRIMARY KEY,
            plant_name VARCHAR(255),
            client_name VARCHAR(255),
            type ENUM('solar', 'wind') NOT NULL
        )""",
        
        """CREATE TABLE IF NOT EXISTS tbl_generation (
            id INT AUTO_INCREMENT PRIMARY KEY,
            plant_id VARCHAR(50) NOT NULL,
            plant_name VARCHAR(255),
            client_name VARCHAR(255),
            type ENUM('solar', 'wind') NOT NULL,
            date DATE NOT NULL,
            time TIME NOT NULL,
            datetime DATETIME GENERATED ALWAYS AS (TIMESTAMP(date, time)) STORED,
            generation DECIMAL(10, 2),
            active_power DECIMAL(10, 2),
            UNIQUE KEY uq_gen (plant_id, date, time, type)
        )""",
        
        """CREATE TABLE IF NOT EXISTS tbl_consumption (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cons_unit VARCHAR(100) NOT NULL,
            client_name VARCHAR(255),
            date DATE NOT NULL,
            time TIME NOT NULL,
            datetime DATETIME GENERATED ALWAYS AS (TIMESTAMP(date, time)) STORED,
            consumption DECIMAL(10, 2),
            UNIQUE KEY uq_cons (cons_unit, date, time)
        )""",
        
        """CREATE TABLE IF NOT EXISTS consumption_mapping (
            id INT AUTO_INCREMENT PRIMARY KEY,
            client_name VARCHAR(255),
            cons_unit VARCHAR(100),
            location_name VARCHAR(255),
            percentage DECIMAL(5,2),
            UNIQUE KEY uq_cons_pct (client_name, cons_unit)
        )""",
        
        """CREATE TABLE IF NOT EXISTS settlement_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            plant_id VARCHAR(50) NOT NULL,
            client_name VARCHAR(255),
            cons_unit VARCHAR(100),
            type ENUM('solar', 'wind') NOT NULL,
            date DATE NOT NULL,
            time TIME NOT NULL,
            datetime DATETIME GENERATED ALWAYS AS (TIMESTAMP(date, time)) STORED,
            generation DECIMAL(10, 2),
            consumption DECIMAL(10, 2),
            surplus_demand DECIMAL(10, 2),
            surplus_deficit DECIMAL(10, 2),
            UNIQUE KEY uq_settle (plant_id, date, time, type)
        )"""
    ]
    
    try:
        print("Dropping existing tables...")
        for drop_sql in drop_tables:
            try:
                session.execute(text(drop_sql))
                print(f"Executed: {drop_sql}")
            except Exception as e:
                print(f"Warning dropping table: {e}")
        session.commit()
        print("Existing tables dropped successfully!")
        
        print("Creating new tables...")
        for create_sql in create_tables:
            try:
                session.execute(text(create_sql))
                table_name = create_sql.split("TABLE IF NOT EXISTS ")[1].split(" ")[0]
                print(f"Created table: {table_name}")
            except Exception as e:
                print(f"Error creating table: {e}")
                raise
        
        session.commit()
        print("New tables created successfully!")
        
        # Verify tables were created
        result = session.execute(text("SHOW TABLES"))
        tables = [row[0] for row in result.fetchall()]
        print(f"Tables in database: {tables}")
        
    except Exception as e:
        session.rollback()
        print(f"Error creating tables: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    create_tables()