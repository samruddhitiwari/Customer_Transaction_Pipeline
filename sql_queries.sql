# a) Monthly Spend Per Customer:
"""
SELECT customer_id, 
       DATE_TRUNC('month', timestamp) as month,
       SUM(amount) as monthly_spend
FROM transactions
GROUP BY customer_id, month
ORDER BY customer_id, month;
"""

# b) Detect Unusual Transactions (e.g., amount > 4000)
"""
SELECT * FROM transactions
WHERE amount > 4000;
"""

# c) KMeans Clustering (Optional ML with scikit-learn)
from sklearn.cluster import KMeans

agg = transactions_df.groupby('customer_id')["amount"].agg(["count", "sum", "mean"]).reset_index()
kmeans = KMeans(n_clusters=3)
agg['cluster'] = kmeans.fit_predict(agg[["count", "sum", "mean"]])
