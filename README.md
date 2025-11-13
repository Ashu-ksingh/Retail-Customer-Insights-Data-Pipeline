# 🛍️ Retail Customer Insights Data Pipeline (PySpark + PostgreSQL)

This project builds an end-to-end **batch ETL pipeline** to generate customer-level insights for a retail business.  
The pipeline uses **PySpark** for data processing and **PostgreSQL** as the storage layer for the final curated data.

It processes multi-source data (CSV + JSON), performs transformations, enrichments, aggregations, and loads output into a PostgreSQL table.

---

## 🧠 Overview

The pipeline processes three raw datasets:

- `orders.csv` (order details)
- `customers.csv` (customer demographics)
- `products.json` (product master)

It generates customer-level insights such as:

- Total amount spent  
- Total number of orders  
- Average product rating  
- Customer city insights  

The final output is stored in PostgreSQL for analytics.

---

## 🏗️ Architecture
Raw Data (CSV + JSON)
↓
extract.py → Load raw data using PySpark
transform.py → Clean, transform & aggregate customer insights
load.py → Load final output into PostgreSQL
main.py → Run full pipeline end-to-end

---

## 🧰 Tech Stack

- **PySpark**
- **Python**
- **PostgreSQL**
- **SQLAlchemy + psycopg2**
- **CSV + JSON data sources**

---

## 📂 Project Structure

retail_customer_insights/
│
├── data/
│ ├── orders.csv
│ ├── customers.csv
│ └── products.json
│
├── scripts/
│ ├── extract.py # Reads raw input data using PySpark
│ ├── transform.py # Clean + join + aggregate to produce insights
│ ├── load.py # Loads final curated dataframe into PostgreSQL
│ └── main.py # Orchestrator: extract → transform → load
│
├── config/
│ └── db_config.py # PostgreSQL connection info
│
├── requirements.txt
└── README.md

---

## 🧰 Setup Instructions

```bash
# 1️⃣ Install dependencies
pip install -r requirements.txt

# 2️⃣ Run only extraction
python scripts/extract.py

# 3️⃣ Run transformation
python scripts/transform.py

# 4️⃣ Load curated data into PostgreSQL
python scripts/load.py

# 5️⃣ (Optional) Run complete end-to-end pipeline
python scripts/main.py

## 🗃️ PostgreSQL Output Table

Table Name: customer_insights

Column	Description
customer_id	Unique customer identifier
name	Customer name
customer_city	City extracted from customers.csv
total_spent	Total order value
total_orders	Number of orders placed
avg_rating	Average rating across all purchases

## ⚙️ PySpark Transformations Performed

Load CSV + JSON input data
Handle nulls & cast datatypes

Join:
orders ↔ customers
orders ↔ products

Add derived fields:
total_amount = quantity × price

Compute customer metrics:
total_spent
total_orders
avg_rating

Return final curated customer_insights dataframe

## 💾 Example Output
+-----------+--------+-------------+-----------+------------+----------+
|customer_id| name   |customer_city|total_spent|total_orders|avg_rating|
+-----------+--------+-------------+-----------+------------+----------+
|        101| Neha   | Mumbai      |     2000  |     2      |   5.0    |
|        102| Ashok  | Delhi       |     1800  |     2      |   4.0    |
|        103| Raj    | Bangalore   |     1200  |     1      |   3.0    |
+-----------+--------+-------------+-----------+------------+----------+

