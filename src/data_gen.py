import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import random
import uuid
import os
from .models import Company, Employee, EmployeeEvent, JobPosting, MarketData, SeniorityLevel, EventType, Base

# Configuration
NUM_COMPANIES = 10
START_DATE = datetime(2019, 1, 1)
END_DATE = datetime(2021, 1, 1)
COVID_START = datetime(2020, 2, 20)
COVID_END = datetime(2020, 4, 15)

DB_PATH = 'sqlite:///workforce_alpha/data/db/quant.db'

def get_random_seniority():
    r = random.random()
    if r < 0.6: return SeniorityLevel.JUNIOR
    if r < 0.85: return SeniorityLevel.MID
    if r < 0.95: return SeniorityLevel.SENIOR
    return SeniorityLevel.EXEC

def generate_mock_data():
    # Ensure directory exists
    os.makedirs(os.path.dirname(DB_PATH.replace('sqlite:///', '')), exist_ok=True)
    
    engine = create_engine(DB_PATH)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    session = Session(engine)

    print("Generating Companies...")
    companies = []
    for i in range(NUM_COMPANIES):
        health_score = random.uniform(0.1, 0.9) # 0.1 (Sick) to 0.9 (Healthy)
        ticker = f"MOCK_{i:02d}"
        company = Company(
            ticker=ticker,
            sector="Technology",
            industry="Software",
        )
        session.add(company)
        session.flush() # Get ID
        
        # Initial State
        initial_employees = []
        for _ in range(random.randint(50, 150)):
            emp = Employee(
                company_id=company.id,
                anonymized_hash=str(uuid.uuid4()),
                current_seniority=get_random_seniority()
            )
            session.add(emp)
            initial_employees.append(emp)
        
        companies.append({
            'obj': company, 
            'health': health_score, 
            'price': 100.0,
            'employees': initial_employees,
            'open_roles': random.randint(5, 20)
        })
    
    session.commit()

    print("Simulating Daily History...")
    current_date = START_DATE
    total_days = (END_DATE - START_DATE).days
    
    for day_idx in range(total_days + 1):
        if day_idx % 30 == 0:
            print(f"Processing {current_date.date()}...")
            
        is_covid = COVID_START <= current_date <= COVID_END
        
        for co_data in companies:
            company = co_data['obj']
            health = co_data['health']
            current_employees = co_data['employees']
            
            # --- 1. Market Data Generation ---
            # Market Factor
            market_return = np.random.normal(0.0005, 0.005) # Reduced market noise
            if is_covid:
                market_return = np.random.normal(-0.02, 0.02) # Crash
            
            # Idiosyncratic Factor (Alpha)
            # Increased alpha multiplier from 0.001 to 0.003 for stronger signal
            alpha = (health - 0.5) * 0.003 
            if is_covid:
                alpha = (health - 0.5) * 0.02 # Stronger divergence during stress
            
            # Reduced idiosyncratic noise from 0.01 to 0.005
            daily_return = market_return + alpha + np.random.normal(0, 0.005)
            
            # Update Price
            co_data['price'] *= (1 + daily_return)
            co_data['price'] = max(0.01, co_data['price']) # No bankruptcy
            
            market_entry = MarketData(
                company_id=company.id,
                date=current_date.date(),
                close=co_data['price'],
                adjusted_close=co_data['price'],
                volume=random.randint(1000, 100000)
            )
            session.add(market_entry)
            
            # --- 2. Employee Events ---
            # Base Probabilities
            # Increased sensitivity to health
            join_prob = 0.005 * (health ** 2) # Healthy hire much more (quadratic)
            leave_prob = 0.005 * ((1 - health) ** 2) # Sick lose much more (quadratic)
            
            if is_covid:
                join_prob *= 0.05 # Hiring Freeze (almost total)
                leave_prob *= 3.0 # Layoffs (severe)
            
            # Joins
            if random.random() < join_prob * len(current_employees):
                new_emp = Employee(
                    company_id=company.id,
                    anonymized_hash=str(uuid.uuid4()),
                    current_seniority=get_random_seniority()
                )
                session.add(new_emp)
                current_employees.append(new_emp)
                
                event = EmployeeEvent(
                    employee=new_emp, # Associate with the new object instance
                    event_date=current_date.date(),
                    event_type=EventType.JOIN,
                    metadata_json={}
                )
                session.add(event)
            
            # Leaves
            if len(current_employees) > 0 and random.random() < leave_prob * len(current_employees):
                leaver = random.choice(current_employees)
                current_employees.remove(leaver)
                
                event = EmployeeEvent(
                    employee_id=leaver.id, # Use ID if flushed, but object is safer if attached
                    event_date=current_date.date(),
                    event_type=EventType.LEAVE,
                    metadata_json={}
                )
                # Need to ensure leaver is in session or use ID. 
                # Since we created them in this session, passing the object works if it's attached.
                # However, for initial employees, they are committed.
                # Let's just use employee_id.
                event.employee = leaver
                session.add(event)

            # Title Changes / Promotions
            if len(current_employees) > 0 and random.random() < 0.001:
                promoted = random.choice(current_employees)
                event = EmployeeEvent(
                    employee=promoted,
                    event_date=current_date.date(),
                    event_type=EventType.TITLE_CHANGE,
                    metadata_json={"old_title": "Analyst", "new_title": "Associate"}
                )
                session.add(event)
                
            # --- 3. Job Postings ---
            # Mean reversion to target size based on health
            target_open_roles = int(20 * health)
            if is_covid: target_open_roles = 0
            
            current_open = co_data['open_roles']
            change = int((target_open_roles - current_open) * 0.1) + random.randint(-1, 1)
            new_open = max(0, current_open + change)
            
            new_added = max(0, new_open - current_open) if new_open > current_open else 0
            closed = max(0, current_open - new_open) if new_open < current_open else 0
            
            co_data['open_roles'] = new_open
            
            posting = JobPosting(
                company_id=company.id,
                date=current_date.date(),
                total_open_roles=new_open,
                new_roles_added=new_added,
                roles_closed=closed
            )
            session.add(posting)

        current_date += timedelta(days=1)
        
        # Periodic commit to keep memory usage low
        if day_idx % 30 == 0:
            session.commit()
            
    session.commit()
    print("Data Generation Complete.")

if __name__ == "__main__":
    generate_mock_data()
