import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from datetime import datetime
import sys
import os
from .models import Company, MarketData, EmployeeEvent, EventType, Base

DB_PATH = 'sqlite:///workforce_alpha/data/db/quant.db'

def ingest_market_data(tickers):
    engine = create_engine(DB_PATH)
    session = Session(engine)
    
    print(f"Fetching market data for {tickers}...")
    
    # Download data
    data = yf.download(tickers, start="2020-01-01", end=datetime.now().strftime('%Y-%m-%d'), group_by='ticker')
    
    for ticker in tickers:
        # Get or create company
        company = session.query(Company).filter_by(ticker=ticker).first()
        if not company:
            print(f"Creating company {ticker}...")
            company = Company(ticker=ticker, sector="Unknown", industry="Unknown")
            session.add(company)
            session.commit()
            
        # Extract ticker data
        if len(tickers) > 1:
            df = data[ticker].copy()
        else:
            df = data.copy()
            
        df = df.dropna()
        
        print(f"Saving {len(df)} records for {ticker}...")
        
        # Bulk insert
        market_entries = []
        for index, row in df.iterrows():
            # Check if exists
            exists = session.query(MarketData).filter_by(company_id=company.id, date=index.date()).first()
            if not exists:
                market_entries.append(MarketData(
                    company_id=company.id,
                    date=index.date(),
                    close=row['Close'],
                    adjusted_close=row['Adj Close'],
                    volume=row['Volume']
                ))
        
        if market_entries:
            session.bulk_save_objects(market_entries)
            session.commit()
            
    print("Market data ingestion complete.")

def ingest_employee_events(csv_path):
    engine = create_engine(DB_PATH)
    session = Session(engine)
    
    print(f"Loading employee events from {csv_path}...")
    df = pd.read_csv(csv_path)
    
    # Expected CSV columns: ticker, event_date, event_type, seniority, metadata
    
    for _, row in df.iterrows():
        ticker = row['ticker']
        company = session.query(Company).filter_by(ticker=ticker).first()
        if not company:
            print(f"Warning: Company {ticker} not found. Skipping.")
            continue
            
        # Create Event
        # Note: In a real system, we'd map to specific employees. 
        # Here we might create dummy employees if ID not provided.
        
        event = EmployeeEvent(
            # employee_id=... # We need an employee. 
            # For simplicity in this ingestion, we'll create a dummy employee or look up if provided.
            # Let's assume CSV has employee_hash
            event_date=pd.to_datetime(row['event_date']).date(),
            event_type=EventType[row['event_type']], # Assumes matches Enum
            metadata_json=row.get('metadata', {})
        )
        
        # Handle Employee
        # This part depends heavily on the CSV format. 
        # For now, we'll just print a placeholder as we don't have the CSV spec from the user yet.
        pass
        
    print("Employee event ingestion logic placeholder.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "market":
            tickers = sys.argv[2:]
            ingest_market_data(tickers)
        elif command == "events":
            csv_path = sys.argv[2]
            ingest_employee_events(csv_path)
    else:
        print("Usage: python -m src.ingest [market TICKER1 TICKER2 | events CSV_PATH]")
