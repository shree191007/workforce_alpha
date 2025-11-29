import pandas as pd
import numpy as np
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from .models import Company, EmployeeEvent, JobPosting, DailyFactor, EventType, SeniorityLevel, Base

DB_PATH = 'sqlite:///workforce_alpha/data/db/quant.db'

def compute_signals():
    engine = create_engine(DB_PATH)
    session = Session(engine)
    
    print("Loading data...")
    # Load all events
    events_df = pd.read_sql(select(EmployeeEvent), session.bind)
    events_df['event_date'] = pd.to_datetime(events_df['event_date'])
    
    # Convert Enum to string to avoid comparison errors
    events_df['event_type'] = events_df['event_type'].apply(lambda x: x.name if hasattr(x, 'name') else str(x).split('.')[-1])

    # Load job postings
    jobs_df = pd.read_sql(select(JobPosting), session.bind)
    jobs_df['date'] = pd.to_datetime(jobs_df['date'])
    
    # Load companies
    companies_df = pd.read_sql(select(Company), session.bind)
    company_ids = companies_df['id'].unique()
    
    # Load Employees
    employees_df = pd.read_sql("SELECT id, company_id, current_seniority FROM employees", session.bind)
    
    # Merge events with employee info
    events_df = events_df.merge(employees_df, left_on='employee_id', right_on='id', how='left')
    
    all_factors = []
    
    print("Computing factors per company...")
    for company_id in company_ids:
        # Filter for company
        co_events = events_df[events_df['company_id'] == company_id].copy()
        co_jobs = jobs_df[jobs_df['company_id'] == company_id].copy().set_index('date')
        
        if co_events.empty:
            continue
            
        # Create daily index
        min_date = co_events['event_date'].min()
        max_date = co_events['event_date'].max()
        dates = pd.date_range(min_date, max_date)
        daily_df = pd.DataFrame(index=dates)
        
        # Daily Counts
        daily_counts = co_events.groupby(['event_date', 'event_type']).size().unstack(fill_value=0)
        daily_counts = daily_counts.reindex(dates, fill_value=0)
        
        # Ensure columns exist (using string names)
        for et in ['JOIN', 'LEAVE', 'PROMOTION', 'TITLE_CHANGE']:
            if et not in daily_counts.columns:
                daily_counts[et] = 0
                
        # Exec Events
        # Convert seniority to string if needed
        co_events['current_seniority_str'] = co_events['current_seniority'].apply(lambda x: x.name if hasattr(x, 'name') else str(x).split('.')[-1])
        
        exec_daily = co_events[co_events['current_seniority_str'] == 'EXEC'].groupby(['event_date', 'event_type']).size().unstack(fill_value=0)
        exec_daily = exec_daily.reindex(dates, fill_value=0)
        
        # Fill missing columns for execs
        for et in ['JOIN', 'LEAVE', 'PROMOTION', 'TITLE_CHANGE']:
            if et not in exec_daily.columns:
                exec_daily[et] = 0

        # Headcount Calculation
        current_headcount = len(employees_df[employees_df['company_id'] == company_id])
        headcount_series = []
        curr = current_headcount
        
        # Reverse iterate
        for date in reversed(dates):
            joins = daily_counts.loc[date].get('JOIN', 0)
            leaves = daily_counts.loc[date].get('LEAVE', 0)
            headcount_series.append(curr)
            curr = curr - joins + leaves
            
        headcount_series = pd.Series(headcount_series[::-1], index=dates)
        
        # Rolling Metrics (30D)
        rolling_joins = daily_counts['JOIN'].rolling(30).sum()
        rolling_leaves = daily_counts['LEAVE'].rolling(30).sum()
        rolling_promos = (daily_counts.get('PROMOTION', 0) + daily_counts.get('TITLE_CHANGE', 0)).rolling(30).sum()
        
        rolling_exec_churn = (exec_daily.get('JOIN', 0) + exec_daily.get('LEAVE', 0)).rolling(30).sum()
        
        # Factors
        # PEV
        pev = rolling_promos / headcount_series.replace(0, 1)
        
        # EXI (Corrected: Leaves / Joins)
        exi = (rolling_leaves + 1) / (rolling_joins + 1)
        
        # Hiring Momentum
        daily_jobs = daily_df.join(co_jobs['total_open_roles']).fillna(method='ffill').fillna(0)
        j_t = daily_jobs['total_open_roles']
        j_t_30 = j_t.shift(30)
        hiring_mom = (j_t - j_t_30) / (j_t_30 + 1)
        
        # SLV
        # Count execs
        total_execs = len(employees_df[(employees_df['company_id'] == company_id) & (employees_df['current_seniority'].apply(lambda x: str(x).endswith('EXEC')))])
        if total_execs == 0: total_execs = 1
        slv = rolling_exec_churn / total_execs
        
        # Combine
        company_factors = pd.DataFrame({
            'company_id': company_id,
            'pev_score': pev,
            'exodus_score': exi,
            'hiring_freeze_score': hiring_mom,
            'exec_volatility': slv
        })
        all_factors.append(company_factors)
        
    # Combine all
    if not all_factors:
        print("No factors computed.")
        return

    full_df = pd.concat(all_factors)
    full_df = full_df.dropna() # Drop first 30 days
    
    print("Computing Z-Scores and WSI...")
    # Z-Score Cross-Sectionally per Date
    def zscore(x):
        if x.std() == 0: return 0
        return (x - x.mean()) / x.std()
    
    full_df = full_df.reset_index().rename(columns={'index': 'date'})
    
    full_df['z_pev'] = full_df.groupby('date')['pev_score'].transform(zscore)
    full_df['z_exi'] = full_df.groupby('date')['exodus_score'].transform(zscore)
    full_df['z_hiring'] = full_df.groupby('date')['hiring_freeze_score'].transform(zscore)
    full_df['z_slv'] = full_df.groupby('date')['exec_volatility'].transform(zscore)
    
    # WSI = PEV + EXI - Hiring + SLV
    full_df['wsi_composite'] = full_df['z_pev'] + full_df['z_exi'] - full_df['z_hiring'] + full_df['z_slv']
    
    # Save to DB
    print("Saving to DB...")
    session.query(DailyFactor).delete()
    
    factors_to_save = []
    for _, row in full_df.iterrows():
        factors_to_save.append(DailyFactor(
            company_id=int(row['company_id']),
            date=row['date'].date(),
            pev_score=row['pev_score'],
            exodus_score=row['exodus_score'],
            hiring_freeze_score=row['hiring_freeze_score'],
            exec_volatility=row['exec_volatility'],
            wsi_composite=row['wsi_composite']
        ))
    
    session.bulk_save_objects(factors_to_save)
    session.commit()
    print("Signal Processing Complete.")

if __name__ == "__main__":
    compute_signals()
