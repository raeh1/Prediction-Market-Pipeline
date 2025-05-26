# Prediction Market Pipeline

A lightweight ETL pipeline for extracting, transforming, and storing prediction market data.

## Features

- Modular ETL architecture
- Source-to-database automation
- GitHub Actions CI support

## Installation

```bash
git clone https://github.com/yourname/project.git
cd project
pip install -r requirements.txt
```

## Conifg
create a .env file a the root directory with the following variables

```python
POLYMARKET_HOST = your database host
POLYMARKET_DATABASE = your database name
POLYMARKET_USER  = your database user
POLYMARKET_PASSWORD = your database password
POLYMARKET_PORT = your database port
```