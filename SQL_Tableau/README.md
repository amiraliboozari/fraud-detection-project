# 📊 SQL + Tableau Fraud Detection Dashboard

This section of the project focuses on integrating **SQL data analysis** with **Tableau visualization** to assess data integrity and fraud detection model performance.

---

## 🧮 Dataset Overview

- **Total Transactions:** ~6,407,000  
- **Total Chunks:** 641  
- Each **chunk ID** represents a batch of transactions processed sequentially in the dataset, grouped to simplify analysis and performance tracking.

---

## ⚖️ Balance Consistency Analysis

This dashboard identifies discrepancies between **origin** and **destination** transaction balances.

| Metric | Value |
|--------|-------:|
| **Total Origin Inconsistencies** | 15,258,258 |
| **Total Destination Inconsistencies** | 10,324,230 |

**Interpretation:**  
A higher inconsistency at the origin side indicates that mismatches occur more frequently at transaction initiation. These inconsistencies may signal data quality issues or reporting errors that can impact downstream fraud detection accuracy.

### 📉 Visualization
![Data Integrity Overview](../images/Data%20Integrity%20Overview.png)

---

## 🚨 Fraud Detection Overview

This dashboard evaluates how effectively the fraud detection system identifies and flags suspicious activity.

| Metric | Value |
|--------|-------:|
| **Total Frauds** | 8,307 |
| **Correctly Flagged Frauds** | 16 |
| **Missed Frauds** | 8,291 |
| **Detection Rate** | 0.19 (19%) |
| **False Positive Rate** | ~0.01 |

**Summary:**  
While the system identifies some true frauds, the high number of missed frauds indicates opportunities to improve recall and fine-tune detection thresholds.

### 📊 Visualization
![Fraud Detection Overview](../images/Fraud%20Dectection%20Overview.png)

---

## 📈 Fraud Detection Performance Insights

This dashboard provides a deeper look at model performance, visualizing detection vs. false positive trade-offs and comparing flagged vs. confirmed fraud cases.

**Key Insights:**
1. **Flagged transactions** exceed confirmed frauds — the model prioritizes caution.
2. **Some flagged transactions are false positives**, highlighting the precision-recall trade-off.
3. **Fraud is consistent** across transaction types, not concentrated in one area.
4. The **gap between flagged and confirmed frauds** suggests potential model improvements.

**Conclusion:**  
The analysis shows the model is cautious but may under-detect fraud. Adjusting detection thresholds could improve accuracy and recall balance.

### 📊 Visualization
![Fraud Detection Performance Insights](../images/Fraud%20Dectection%20Perfomence%20Insights.png)

---

## 🧠 Key Takeaways
- Data validation via SQL ensures reliable inputs for machine learning models.  
- Tableau visualizations highlight both **data integrity** and **fraud detection performance**.  
- Insights from this dashboard guide better fraud prevention strategies and operational monitoring.

---

### 📁 Folder Contents
- `SQL_Queries/` – Contains SQL scripts for data preprocessing and aggregation.  
- `Dashboards/` – Tableau workbooks and exported visuals (`.twb` / `.png` files).  
- `README.md` – This documentation.

---

*Created as part of the Advanced Data Analytics Capstone Project.*
