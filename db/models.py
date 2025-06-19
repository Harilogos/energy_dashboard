"""
Database models for the Energy Generation Dashboard.
"""
from sqlalchemy import create_engine, Column, String, Enum, DateTime, DECIMAL, Integer, Date, Time, text, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TblPlants(Base):
    __tablename__ = 'tbl_plants'
    plant_id = Column(String(50), primary_key=True)
    plant_name = Column(String(255))
    client_name = Column(String(255))
    type = Column(Enum('solar', 'wind'), nullable=False)

class TblGeneration(Base):
    __tablename__ = 'tbl_generation'
    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_id = Column(String(50), nullable=False)
    plant_name = Column(String(255))
    client_name = Column(String(255))
    type = Column(Enum('solar', 'wind'), nullable=False)
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    datetime = Column(DateTime, server_default=text("(TIMESTAMP(date, time))"))
    generation = Column(DECIMAL(10, 2))
    active_power = Column(DECIMAL(10, 2))
    pr = Column(DECIMAL(5, 2))
    poa = Column(DECIMAL(10, 2))
    avg_wind_speed = Column(DECIMAL(5, 2))
    
    __table_args__ = (
        UniqueConstraint('plant_id', 'date', 'time', 'type', name='uq_gen'),
    )

class TblConsumption(Base):
    __tablename__ = 'tbl_consumption'
    id = Column(Integer, primary_key=True, autoincrement=True)
    cons_unit = Column(String(100), nullable=False)
    client_name = Column(String(255))
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    datetime = Column(DateTime, server_default=text("(TIMESTAMP(date, time))"))
    consumption = Column(DECIMAL(10, 2))
    
    __table_args__ = (
        UniqueConstraint('cons_unit', 'date', 'time', name='uq_cons'),
    )

class ConsumptionMapping(Base):
    __tablename__ = 'consumption_mapping'
    id = Column(Integer, primary_key=True, autoincrement=True)
    client_name = Column(String(255))
    cons_unit = Column(String(100))
    location_name = Column(String(255))
    percentage = Column(DECIMAL(5, 2))
    
    __table_args__ = (
        UniqueConstraint('client_name', 'cons_unit', name='uq_cons_pct'),
    )

class SettlementData(Base):
    __tablename__ = 'settlement_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_id = Column(String(50), nullable=False)
    client_name = Column(String(255))
    cons_unit = Column(String(100))
    type = Column(Enum('solar', 'wind'), nullable=False)
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    datetime = Column(DateTime, server_default=text("(TIMESTAMP(date, time))"))
    allocated_generation = Column(DECIMAL(10, 2))
    consumption = Column(DECIMAL(10, 2))
    deficit = Column(DECIMAL(10, 2))
    surplus_demand = Column(DECIMAL(10, 2))
    surplus_generation = Column(DECIMAL(10, 2))
    settled = Column(DECIMAL(10, 2))
    slot_name = Column(String(50))
    slot_time = Column(String(20))

    
    
    __table_args__ = (
        UniqueConstraint('plant_id', 'date', 'time', 'type', name='uq_settle'),
    )


class BankingSettlement(Base):
    __tablename__ = 'banking_settlement'

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_name = Column(String(255), nullable=False)
    plant_name = Column(String(255), nullable=False)
    date = Column(Date, nullable=False)
    type = Column(Enum('solar', 'wind'), nullable=False)
    cons_unit = Column(String(100), nullable=False)
    slot_name = Column(String(50), nullable=False)
    slot_time = Column(String(20), nullable=False)

    # Initial aggregated values
    surplus_demand_sum = Column(DECIMAL(12, 2), default=0.00)
    surplus_generation_sum = Column(DECIMAL(12, 2), default=0.00)
    matched_settled_sum = Column(DECIMAL(12, 2), default=0.00)

    # After intra settlement
    surplus_generation_sum_after_intra = Column(DECIMAL(12, 2), default=0.00)
    surplus_demand_sum_after_intra = Column(DECIMAL(12, 2), default=0.00)
    intra_settlement = Column(DECIMAL(12, 2), default=0.00)

    # After inter settlement
    surplus_generation_sum_after_inter = Column(DECIMAL(12, 2), default=0.00)
    surplus_demand_sum_after_inter = Column(DECIMAL(12, 2), default=0.00)
    inter_settlement = Column(DECIMAL(12, 2), default=0.00)

    

    __table_args__ = (
        UniqueConstraint('client_name', 'plant_name', 'cons_unit', 'slot_name', 'date', 'type', name='uq_banking_stage'),
    )