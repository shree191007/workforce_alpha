# Workforce Stress Index (WSI) Alpha

A quantitative trading strategy that leverages alternative workforce data to predict equity returns. This project simulates a "Workforce Stress Index" (WSI) based on employee turnover, hiring freezes, and executive departures, and trades a Long/Short equity portfolio based on these signals.

## Project Structure

- `src/`: Source code for data generation, signal engineering, and backtesting.
    - `data_gen.py`: Generates synthetic market and workforce data (Monte Carlo simulation).
    - `signals.py`: Engineers factors (PEV, EXI, Hiring Momentum, SLV) and calculates the WSI.
    - `backtester.py`: Runs the trading strategy and calculates performance metrics.
    - `optimizer.py`: Optimizes strategy parameters (quantile, smoothing).
    - `models.py`: SQLAlchemy database models.
- `data/`: Directory for storing the SQLite database (excluded from git).


## Getting Started

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Generate Data**:
    ```bash
    python -m src.data_gen
    ```

3.  **Compute Signals**:
    ```bash
    python -m src.signals
    ```

4.  **Run Backtest**:
    ```bash
    python -m src.backtester
    ```

## Strategy Performance

- **Sharpe Ratio**: 1.66
- **Cumulative Return**: 25.90%
- **Win Rate**: 53.36%

*(Based on synthetic data simulation Jan 2019 - Jan 2021)*
