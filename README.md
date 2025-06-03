# Prediction Market Pipeline

A lightweight ETL pipeline for extracting, transforming, and storing prediction market data from sources such as Polymarket to your own database

## Features

- Modular ETL architecture
- Source-to-database automation

## Installation

```bash
git clone https://github.com/raeh1/Prediction-Market-Pipeline.git
pip install -r requirements.txt
```

## Configuration
Create a .env file at the root directory with the following variables

```python
POLYMARKET_HOST = your database host
POLYMARKET_DATABASE = your database name
POLYMARKET_USER  = your database user
POLYMARKET_PASSWORD = your database password
POLYMARKET_PORT = your database port
```

## Function
Use a cron job all other scheduling methods to run etl/main.py routinely