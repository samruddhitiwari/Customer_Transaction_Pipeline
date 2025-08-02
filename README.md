#  Customer Transaction Data Pipeline (Banking Simulation)

This project simulates and processes customer banking transactions, building a full data pipeline from data generation to analytics. It is designed to showcase key data engineering and analytical skills, especially relevant for roles in finance and banking, like Associate Data Engineer at IDFC First Bank.

---

##  Features

*  **Simulated Data Generation**: Use Python & `Faker` to generate realistic customer and transaction data.
*  **ETL Pipeline**: Clean, transform, and load data using Python and Pandas.
*  **Database Integration**: Store processed data in PostgreSQL/MySQL/BigQuery.
*  **SQL Analytics**:

  * Monthly spending per customer
  * Unusual transaction detection
  * Customer segmentation (optional)
*  **Dashboard (Optional)**: Visualize trends using Streamlit or Power BI.

---

##  Tech Stack

* Python 3.9+
* Pandas
* Faker
* PostgreSQL or MySQL
* SQL (PostgreSQL dialect)
* Optional: Streamlit / Power BI / scikit-learn

---

##  Project Structure

```bash
customer-transaction-pipeline/
│
├── data/
│   ├── customers.csv
│   └── transactions.csv
│
├── etl/
│   ├── data_generator.py       # Faker-based data generator
│   ├── transform.py            # Cleaning and transformation
│   └── load_to_db.py           # PostgreSQL insert script
│
├── analysis/
│   ├── monthly_spending.sql
│   ├── fraud_detection.sql
│   └── customer_segmentation.py
│
├── dashboard/ (optional)
│   └── streamlit_app.py
│
├── requirements.txt
└── README.md
```

---

##  Getting Started

### 1. Clone the Repo

```bash
git clone https://github.com/yourusername/customer-transaction-pipeline.git
cd customer-transaction-pipeline
```

### 2. Set Up Environment

```bash
pip install -r requirements.txt
```

### 3. Generate Synthetic Data

```bash
python etl/data_generator.py
```

Generates `customers.csv` and `transactions.csv`.

### 4. Run ETL

```bash
python etl/transform.py
python etl/load_to_db.py
```

Ensure your PostgreSQL database is running and credentials are set in `.env`.

---

##  SQL Analysis

Run these SQL scripts on your database:

* **Monthly Spend Per Customer**:

  ```sql
  SELECT customer_id, DATE_TRUNC('month', txn_date) AS month, SUM(amount) AS monthly_spend
  FROM transactions
  GROUP BY customer_id, month;
  ```

* **Unusual Transaction Detection** (simple threshold-based):

  ```sql
  SELECT *
  FROM transactions
  WHERE amount > 100000;
  ```

* **Customer Segmentation (Optional)**:

  ```bash
  python analysis/customer_segmentation.py
  ```

---

##  Dashboard (Optional)

Launch Streamlit:

```bash
streamlit run dashboard/streamlit_app.py
```

---

##  Use Cases

* Banking transaction analysis
* Fraud detection logic
* Customer behavior segmentation
* Showcasing end-to-end data engineering pipeline

---

##  Skills Demonstrated

* Data engineering (ETL, pipelines, database management)
* SQL analytics
* Python scripting
* Realistic simulation of banking systems
* Optional ML integration

---

##  License

MIT License. Free to use, fork, and build upon.

---

Let me know if you'd like this `README.md` converted into a downloadable file or added to the canvas.
