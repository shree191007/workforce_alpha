import pandas as pd
import numpy as np
from .backtester import load_data, run_strategy

def optimize():
    df = load_data()
    
    quantiles = [0.1, 0.2, 0.3, 0.4, 0.5]
    smoothings = [1, 3, 5, 10]
    
    results = []
    
    print(f"Running optimization on {len(quantiles) * len(smoothings)} combinations...")
    
    for q in quantiles:
        for s in smoothings:
            res = run_strategy(df, quantile=q, smoothing=s)
            results.append({
                'quantile': q,
                'smoothing': s,
                'sharpe': res['sharpe'],
                'return': res['return'],
                'volatility': res['volatility'],
                'max_drawdown': res['max_drawdown']
            })
            print(f"Q: {q:.1f}, S: {s} -> Sharpe: {res['sharpe']:.2f}, Ret: {res['return']:.2%}")
            
    results_df = pd.DataFrame(results)
    best_sharpe = results_df.loc[results_df['sharpe'].idxmax()]
    
    print("\nOptimization Complete.")
    print("Best Parameters (by Sharpe):")
    print(best_sharpe)
    
    return best_sharpe

if __name__ == "__main__":
    optimize()
