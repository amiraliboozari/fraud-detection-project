# Fraud Detection Project

This is my capstone project for the Google Advanced Data Analytics Certificate.  
The goal is to analyze financial transactions and detect fraudulent activity using both **Python/Streamlit** and **SQL + Tableau**.

🧠 Project Goals
- Investigate fraudulent transactions using SQL queries.
- Clean and transform over 6 million transaction records.
- Visualize patterns, inconsistencies, and fraud trends with Tableau dashboards.
- Build an ML-powered web app for live fraud detection (coming soon 🚀).

## Dataset
I am using the [Credit Card Fraud Detection dataset on Kaggle](https://www.kaggle.com/datasets/amanalisiddiqui/fraud-detection-dataset).  
The dataset is a little above 400 MB, so it is not stored directly in this repo.  
You can download it yourself from Kaggle if you’d like to reproduce the project.

## Project Versions
- **Python + Streamlit:** Machine learning model, evaluation, and interactive web app.  
- **SQL + Tableau:** Business-focused analysis and dashboards.  

⚙️ Tools & Technologies
- SQL (MySQL): Data ingestion, cleaning, transformation, analysis
- Tableau Public: Dashboard creation and data storytelling
- Python (Planned): Machine learning, Streamlit web app
- GitHub: Version control and project presentation

📈 SQL & Tableau Dashboards

1️⃣ Fraud vs. Flagged Transactions
Compares total fraud cases with flagged transactions to measure system accuracy.
Detection Rate: 0.19% of frauds correctly flagged
False Positive Rate: 0% of non-frauds incorrectly flagged

2️⃣ Balance Inconsistencies Analysis
Evaluates transactional balance discrepancies between sender and receiver accounts.
Detects mismatches between pre- and post-transaction balances
Helps identify potentially fraudulent or failed transfers

3️⃣ Transaction Overview (Future Expansion)
A holistic dashboard exploring transaction volume, frequency, and anomaly trends.

