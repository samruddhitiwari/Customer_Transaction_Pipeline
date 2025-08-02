-- Monthly Spending Analysis Queries
-- Collection of SQL queries for analyzing customer spending patterns

-- 1. Monthly spending trends across all customers
SELECT 
    year,
    month,
    TO_DATE(year || '-' || month || '-01', 'YYYY-MM-DD') as month_date,
    COUNT(DISTINCT customer_id) as active_customers,
    SUM(total_spent) as total_monthly_spending,
    AVG(total_spent) as avg_customer_spending,
    SUM(transaction_count) as total_transactions,
    AVG(transaction_count) as avg_transactions_per_customer,
    SUM(fraud_count) as total_fraud_transactions,
    CASE 
        WHEN SUM(transaction_count) > 0 
        THEN ROUND(SUM(fraud_count)::NUMERIC / SUM(transaction_count) * 100, 2) 
        ELSE 0 
    END as fraud_rate_percent
FROM monthly_spending
GROUP BY year, month
ORDER BY year DESC, month DESC;

-- 2. Top spending customers by month
WITH monthly_rankings AS (
    SELECT 
        customer_id,
        year,
        month,
        total_spent,
        transaction_count,
        ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY total_spent DESC) as spending_rank
    FROM monthly_spending
    WHERE total_spent > 0
)
SELECT 
    mr.year,
    mr.month,
    mr.customer_id,
    c.first_name,
    c.last_name,
    c.customer_segment,
    mr.total_spent,
    mr.transaction_count,
    ROUND(mr.total_spent / mr.transaction_count, 2) as avg_transaction_amount
FROM monthly_rankings mr
JOIN customers c ON mr.customer_id = c.customer_id
WHERE mr.spending_rank <= 10
ORDER BY mr.year DESC, mr.month DESC, mr.spending_rank;

-- 3. Customer spending volatility analysis
WITH customer_spending_stats AS (
    SELECT 
        customer_id,
        COUNT(*) as months_active,
        AVG(total_spent) as avg_monthly_spending,
        STDDEV(total_spent) as spending_stddev,
        MIN(total_spent) as min_monthly_spending,
        MAX(total_spent) as max_monthly_spending,
        SUM(total_spent) as total_lifetime_spending
    FROM monthly_spending
    WHERE total_spent > 0
    GROUP BY customer_id
    HAVING COUNT(*) >= 3  -- At least 3 months of data
)
SELECT 
    css.customer_id,
    c.first_name,
    c.last_name,
    c.customer_segment,
    c.annual_income,
    css.months_active,
    ROUND(css.avg_monthly_spending, 2) as avg_monthly_spending,
    ROUND(css.spending_stddev, 2) as spending_volatility,
    ROUND(css.total_lifetime_spending, 2) as total_lifetime_spending,
    CASE 
        WHEN css.avg_monthly_spending > 0 
        THEN ROUND(css.spending_stddev / css.avg_monthly_spending, 2) 
        ELSE 0 
    END as coefficient_of_variation,
    CASE 
        WHEN css.spending_stddev / css.avg_monthly_spending > 1.0 THEN 'High Volatility'
        WHEN css.spending_stddev / css.avg_monthly_spending > 0.5 THEN 'Medium Volatility'
        ELSE 'Low Volatility'
    END as volatility_category
FROM customer_spending_stats css
JOIN customers c ON css.customer_id = c.customer_id
ORDER BY css.spending_stddev DESC;

-- 4. Seasonal spending patterns
SELECT 
    month,
    CASE 
        WHEN month IN (12, 1, 2) THEN 'Winter'
        WHEN month IN (3, 4, 5) THEN 'Spring'
        WHEN month IN (6, 7, 8) THEN 'Summer'
        WHEN month IN (9, 10, 11) THEN 'Fall'
    END as season,
    COUNT(DISTINCT customer_id) as active_customers,
    AVG(total_spent) as avg_spending,
    SUM(total_spent) as total_spending,
    AVG(transaction_count) as avg_transactions
FROM monthly_spending
GROUP BY month
ORDER BY month;

-- 5. Customer segment spending comparison
SELECT 
    c.customer_segment,
    COUNT(DISTINCT ms.customer_id) as customers_with_spending,
    AVG(ms.total_spent) as avg_monthly_spending,
    SUM(ms.total_spent) as total_segment_spending,
    AVG(ms.transaction_count) as avg_monthly_transactions,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ms.total_spent) as median_spending,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms.total_spent) as p95_spending
FROM customers c
JOIN monthly_spending ms ON c.customer_id = ms.customer_id
WHERE c.customer_segment IS NOT NULL
GROUP BY c.customer_segment
ORDER BY avg_monthly_spending DESC;

-- 6. Month-over-month growth analysis
WITH monthly_totals AS (
    SELECT 
        year,
        month,
        SUM(total_spent) as total_spending,
        COUNT(DISTINCT customer_id) as active_customers
    FROM monthly_spending
    GROUP BY year, month
),
monthly_growth AS (
    SELECT 
        year,
        month,
        total_spending,
        active_customers,
        LAG(total_spending) OVER (ORDER BY year, month) as prev_month_spending,
        LAG(active_customers) OVER (ORDER BY year, month) as prev_month_customers
    FROM monthly_totals
)
SELECT 
    year,
    month,
    total_spending,
    active_customers,
    prev_month_spending,
    CASE 
        WHEN prev_month_spending > 0 
        THEN ROUND(((total_spending - prev_month_spending) / prev_month_spending * 100), 2)
        ELSE NULL 
    END as spending_growth_percent,
    CASE 
        WHEN prev_month_customers > 0 
        THEN ROUND(((active_customers - prev_month_customers)::NUMERIC / prev_month_customers * 100), 2)
        ELSE NULL 
    END as customer_growth_percent
FROM monthly_growth
ORDER BY year DESC, month DESC;

-- 7. Customer churn analysis based on spending activity
WITH customer_last_activity AS (
    SELECT 
        customer_id,
        MAX(year * 12 + month) as last_activity_month,
        (2024 * 12 + 12) as current_month  -- Assuming current date is Dec 2024
    FROM monthly_spending
    GROUP BY customer_id
),
churn_analysis AS (
    SELECT 
        customer_id,
        last_activity_month,
        current_month,
        current_month - last_activity_month as months_inactive,
        CASE 
            WHEN current_month - last_activity_month = 0 THEN 'Active'
            WHEN current_month - last_activity_month <= 2 THEN 'At Risk'
            WHEN current_month - last_activity_month <= 6 THEN 'Churned'
            ELSE 'Long-term Churned'
        END as customer_status
    FROM customer_last_activity
)
SELECT 
    ca.customer_status,
    COUNT(*) as customer_count,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) as percentage,
    AVG(ca.months_inactive) as avg_months_inactive,
    AVG(c.total_amount) as avg_lifetime_value
FROM churn_analysis ca
JOIN customers c ON ca.customer_id = c.customer_id
GROUP BY ca.customer_status
ORDER BY 
    CASE ca.customer_status
        WHEN 'Active' THEN 1
        WHEN 'At Risk' THEN 2
        WHEN 'Churned' THEN 3
        WHEN 'Long-term Churned' THEN 4
    END;

-- 8. Category spending trends by month
SELECT 
    t.year,
    t.month,
    t.category,
    COUNT(DISTINCT t.customer_id) as customers,
    COUNT(*) as transactions,
    SUM(ABS(t.amount)) as total_spent,
    AVG(ABS(t.amount)) as avg_transaction_amount,
    RANK() OVER (PARTITION BY t.year, t.month ORDER BY SUM(ABS(t.amount)) DESC) as category_rank
FROM transactions t
WHERE t.transaction_type = 'Debit' OR t.amount < 0
GROUP BY t.year, t.month, t.category
HAVING SUM(ABS(t.amount)) > 1000  -- Only categories with significant spending
ORDER BY t.year DESC, t.month DESC, total_spent DESC;

-- 9. Weekend vs Weekday spending patterns
SELECT 
    CASE WHEN t.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    t.year,
    t.month,
    COUNT(DISTINCT t.customer_id) as active_customers,
    COUNT(*) as transactions,
    SUM(ABS(t.amount)) as total_spent,
    AVG(ABS(t.amount)) as avg_transaction_amount
FROM transactions t
WHERE t.transaction_type = 'Debit' OR t.amount < 0
GROUP BY 
    CASE WHEN t.is_weekend THEN 'Weekend' ELSE 'Weekday' END,
    t.year, 
    t.month
ORDER BY t.year DESC, t.month DESC, day_type;

-- 10. High-value customer identification
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.customer_segment,
    c.annual_income,
    c.account_balance,
    c.credit_score,
    COUNT(ms.month) as active_months,
    SUM(ms.total_spent) as total_lifetime_spending,
    AVG(ms.total_spent) as avg_monthly_spending,
    MAX(ms.total_spent) as max_monthly_spending,
    SUM(ms.fraud_count) as total_fraud_incidents,
    CASE 
        WHEN SUM(ms.transaction_count) > 0 
        THEN ROUND(SUM(ms.fraud_count)::NUMERIC / SUM(ms.transaction_count) * 100, 4)
        ELSE 0 
    END as personal_fraud_rate
FROM customers c
JOIN monthly_spending ms ON c.customer_id = ms.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_segment, 
         c.annual_income, c.account_balance, c.credit_score
HAVING SUM(ms.total_spent) > 10000  -- High lifetime value customers
ORDER BY total_lifetime_spending DESC
LIMIT 50;
