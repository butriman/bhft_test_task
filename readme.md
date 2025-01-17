
<h1 align="center">bhft_test_task</h1>

<p align="center">
  <b>ETL pipeline for crypto data built with Python.</b>
</p>

<p align="center">
  <!-- Example badges (customize or remove if not needed) -->
  <a href="#">
    <img src="https://img.shields.io/badge/Python-3.10%2B-blue.svg" alt="Python 3.10+"/>
  </a>
  <a href="#">
    <img src="https://img.shields.io/badge/Postgres-Required-green.svg" alt="Postgres Required"/>
  </a>
  <a href="#">
    <img src="https://img.shields.io/badge/Superset-Configured-orange.svg" alt="Superset Configured"/>
  </a>
</p>

<p align="center">
  <a href="#about-the-project">About</a> •
  <a href="#project-structure">Project Structure</a> •
  <a href="#installation-and-usage">Installation & Usage</a> •
  <a href="#notes-on-volume-conversion">Volume Conversion Notes</a>
</p>

---

## About the Project

This repository hosts a Python-based ETL pipeline that collects **cryptocurrency spot data** (candlesticks and general info on instruments) from multiple exchanges. The pipeline supports:

- **Initial Load**: Full ingestion of all available data (limited by API method's restrictions; hardcoded date is 2025-01-01)
- **Incremental Load**: Updating only with newly (T-2) available data
- **Custom Load**: Updating data specified by starting date upon request

Once the raw data is collected, it’s transformed using **pandas**, then loaded into a **Postgres** database (via **SQLAlchemy**). From there, you can explore and visualize the results via a connected **Superset** dashboard.



## Project Structure

This project is organized into clear sections that reflect the ETL workflow:

1. **Data Collection** (`exchange.py`)
   - A set of **classes** that interface with different crypto exchanges
   - Each class implements methods to:
     - Connect to an exchange’s public REST API
     - Fetch the latest **kline** (candlestick) data and **instrument** info details 
   - **List of Exchanges**:
     - `Bybit()`
     - `Binance()`
     - `Gateio()`
     - `Kraken()`
     - `Okx()`

2. **Data Transformation & Loading** (`raw_etl.py`)
    - Classes that handle interaction between **python** and **Postgres DB** via **SQLAlchemy**
        - `RawETLoader` stores methods for insertion and reading (and transforming) RAW data
        - `DmETLoader` stores methods implemention UPSERT strategy when loading transformed data to normalized structures

3. **Database files** (`ddl.sql`)
   - DDL contructions for schemas and table are provided.  
   - **RAW layer**: Contains `exchange_api_kline` and `exchange_api_instrument_info`  
     - Both tables store raw data (API responses) in a JSONB field, are insert-only, and act as a data lake for the project 
   - **DM layer**: Consists of `dim_coin`, `tfct_coin`, and `tfct_exchange_rate`.  
     - They support an **UPSERT** data manipulation strategy:  
       - Rows **unmatched** by a primary key in the source are inserted into the target. 
       - Matched rows update in the target **only** if their insertion timestamp is greater



## Installation and Usage

**Prerequisites**  
> - **Python 3.10+** (earlier versions may work, but are untested)  
> - A **Postgres** database instance
> - A configured **Superset** instance

1. Clone or Download the repository:
   ```bash
   git clone https://github.com/butriman/bhft_test_task.git
   cd bhft_test_task
    ```
2. Install Dependencies:
    ```bash
    pip install -r requirements.txt
    ```
    Make sure you have pandas, SQLAlchemy, and any other required libraries listed in requirements.txt

3. Configure Database Connection

    Postgres DB should be configured as:
    ```bash
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=bhft
    DB_USER=postgres
    DB_PASS=postgres
    ```
    This part is hardcoded in a connection string for now

4. Run the ETL Script

    ```bash
    python main.py --mode initial
    python main.py --mode incremental
    python main.py --mode custom --start_dt 2025-01-05
    ```

5. **Superset** Dashboard

    Assuming **Superset** is already installed after importing config archive *dashboard_spot_trade.zip* (provided in *dashboard_files* folder), there will be a **Spot Trade Dashboard** ready to explore

## Notes on Volume Conversion

There is a block in the project aimed at **converting the volume amount** (the primary metric) to **USDT**.  
> **Currently, this functionality is *not* fully implemented** according to the original request. For now, it only takes spot pairs with a **quote coin** ≠ **USDT** and uses the **quote coin/USDT** instrument (or **USDT/quote coin** if the first is not found) with a volume-weighted average price as the exchange rate. For pairs not found using this logic, there is a **USDT vs Quote** table in the Superset dashboard, allowing you to filter out such pairs by clicking on the **in_USDT** value (via dashboard cross-filtering).

For future development:
- **Fix** volume conversion  
- **Containerize** the project  
- **Julia** port
