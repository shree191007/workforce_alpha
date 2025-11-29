import pandas as pd
import numpy as np
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
import plotly.graph_objects as go
import plotly.io as pio
from .models import Company, DailyFactor, MarketData


DB_PATH = 'sqlite:///workforce_alpha/data/db/quant.db'

def load_data():
    engine = create_engine(DB_PATH)
    session = Session(engine)
    
    print("Loading data for backtest...")
    # Load Factors
    factors_df = pd.read_sql(select(DailyFactor), session.bind)
    factors_df['date'] = pd.to_datetime(factors_df['date'])
    
    # Load Market Data
    market_df = pd.read_sql(select(MarketData), session.bind)
    market_df['date'] = pd.to_datetime(market_df['date'])
    
    # Merge
    df = pd.merge(factors_df, market_df, on=['company_id', 'date'], how='inner')
    df = df.sort_values(['company_id', 'date'])
    return df

def run_strategy(df, quantile=0.3, smoothing=1):
    # Calculate Returns
    df = df.copy()
    df['return'] = df.groupby('company_id')['close'].pct_change()
    
    # Signal Processing
    if smoothing > 1:
        df['wsi_composite'] = df.groupby('company_id')['wsi_composite'].transform(lambda x: x.rolling(window=smoothing).mean())
    
    # Shift Signal: We use WSI from T to trade at T+1
    df['signal_lagged'] = df.groupby('company_id')['wsi_composite'].shift(1)
    
    df = df.dropna()
    
    # Strategy Logic per Day
    dates = np.sort(df['date'].unique())
    
    strategy_returns = []
    
    for d in dates:
        day_df = df[df['date'] == d].copy()
        
        if len(day_df) < 2: 
            continue
            
        n = len(day_df)
        k = max(1, int(n * quantile)) 
        
        day_df = day_df.sort_values('signal_lagged')
        
        longs = day_df.iloc[:k] # Lowest WSI
        shorts = day_df.iloc[-k:] # Highest WSI
        
        long_ret = longs['return'].mean()
        short_ret = shorts['return'].mean()
        
        # Strategy: Long - Short
        strat_ret = 0.5 * long_ret - 0.5 * short_ret
        
        # Benchmark: Equal Weight Market
        mkt_ret = day_df['return'].mean()
        
        strategy_returns.append({'date': d, 'strategy': strat_ret, 'market': mkt_ret})
        
    results_df = pd.DataFrame(strategy_returns).set_index('date')
    
    # Metrics
    if len(results_df) == 0:
        return {'sharpe': 0, 'return': 0, 'df': results_df}

    results_df['cum_strategy'] = (1 + results_df['strategy']).cumprod()
    results_df['cum_market'] = (1 + results_df['market']).cumprod()
    
    annual_factor = 252
    strat_mean = results_df['strategy'].mean() * annual_factor
    strat_std = results_df['strategy'].std() * np.sqrt(annual_factor)
    sharpe = strat_mean / strat_std if strat_std != 0 else 0
    
    total_return = results_df['cum_strategy'].iloc[-1] - 1
    
    return {
        'sharpe': sharpe,
        'return': total_return,
        'volatility': strat_std,
        'max_drawdown': ((results_df['cum_strategy'] - results_df['cum_strategy'].cummax()) / results_df['cum_strategy'].cummax()).min(),
        'win_rate': (results_df['strategy'] > 0).mean(),
        'df': results_df
    }

def run_backtest():
    df = load_data()
    
    print("Simulating Strategy...")
    results = run_strategy(df, quantile=0.4, smoothing=3)
    results_df = results['df']
    
    print(f"Backtest Complete.")
    print(f"Sharpe Ratio: {results['sharpe']:.2f}")
    print(f"Win Rate: {results['win_rate']:.2%}")
    print(f"Annualized Volatility: {results['volatility']:.2%}")
    print(f"Max Drawdown: {results['max_drawdown']:.2%}")
    print(f"Cumulative Return: {results['return']:.2%}")
    
    # Plot
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=results_df.index, y=results_df['cum_strategy'], name='WSI Strategy (L/S)'))
    fig.add_trace(go.Scatter(x=results_df.index, y=results_df['cum_market'], name='Market (Eq Wgt)'))
    
    fig.update_layout(title='Workforce Stress Index Strategy Performance',
                      xaxis_title='Date',
                      yaxis_title='Cumulative Return',
                      template='plotly_dark')
    
    # Save plot
    fig.write_html("workforce_alpha/backtest_results.html")
    print("Plot saved to workforce_alpha/backtest_results.html")

if __name__ == "__main__":
    run_backtest()
