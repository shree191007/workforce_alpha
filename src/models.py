from sqlalchemy import Column, Integer, String, Float, Date, Enum, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, relationship
import enum

Base = declarative_base()

class SeniorityLevel(enum.Enum):
    JUNIOR = "Junior"
    MID = "Mid"
    SENIOR = "Senior"
    EXEC = "Exec"

class EventType(enum.Enum):
    JOIN = "JOIN"
    LEAVE = "LEAVE"
    PROMOTION = "PROMOTION"
    TITLE_CHANGE = "TITLE_CHANGE"

class Company(Base):
    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True)
    ticker = Column(String, unique=True, nullable=False)
    sector = Column(String)
    industry = Column(String)
    
    market_data = relationship("MarketData", back_populates="company")
    employees = relationship("Employee", back_populates="company")
    job_postings = relationship("JobPosting", back_populates="company")
    daily_factors = relationship("DailyFactor", back_populates="company")

class MarketData(Base):
    __tablename__ = 'market_data'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'))
    date = Column(Date, nullable=False)
    close = Column(Float)
    adjusted_close = Column(Float)
    volume = Column(Float)
    
    company = relationship("Company", back_populates="market_data")

class Employee(Base):
    __tablename__ = 'employees'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'))
    anonymized_hash = Column(String, unique=True)
    current_seniority = Column(Enum(SeniorityLevel))
    
    company = relationship("Company", back_populates="employees")
    events = relationship("EmployeeEvent", back_populates="employee")

class JobPosting(Base):
    __tablename__ = 'job_postings'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'))
    date = Column(Date, nullable=False)
    total_open_roles = Column(Integer)
    new_roles_added = Column(Integer)
    roles_closed = Column(Integer)
    
    company = relationship("Company", back_populates="job_postings")

class EmployeeEvent(Base):
    __tablename__ = 'employee_events'
    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey('employees.id'))
    event_date = Column(Date, nullable=False)
    event_type = Column(Enum(EventType))
    metadata_json = Column(JSON)
    
    employee = relationship("Employee", back_populates="events")

class DailyFactor(Base):
    __tablename__ = 'daily_factors'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'))
    date = Column(Date, nullable=False)
    pev_score = Column(Float)
    exodus_score = Column(Float)
    hiring_freeze_score = Column(Float)
    exec_volatility = Column(Float)
    wsi_composite = Column(Float)
    
    company = relationship("Company", back_populates="daily_factors")
