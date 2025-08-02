#!/usr/bin/env python3
"""
Data Transformation and Cleaning Module
Handles data validation, cleaning, and transformation for the banking pipeline
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self):
        self.validation_errors = []
        
    def clean_customers_data(self, customers_df):
        """Clean and validate customer data"""
        logger.info("Starting customer data cleaning...")
        
        df = customers_df.copy()
        initial_rows = len(df)
        
        # Remove duplicates based on email and customer_id
        df = df.drop_duplicates(subset=['customer_id'], keep='first')
        df = df.drop_duplicates(subset=['email'], keep='first')
        
        # Clean and validate email addresses
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        df['email_valid'] = df['email'].str.match(email_pattern, na=False)
        
        # Clean phone numbers - remove non-numeric characters
        df['phone_cleaned'] = df['phone'].str.replace(r'[^\d]', '', regex=True)
        df['phone_cleaned'] = df['phone_cleaned'].str.slice(0, 10)  # Keep only 10 digits
        
        # Validate phone numbers (must be 10 digits)
        df['phone_valid'] = df['phone_cleaned'].str.len() == 10
        
        # Clean zip codes
        df['zip_code'] = df['zip_code'].str.replace(r'[^\d-]', '', regex=True)
        df['zip_code'] = df['zip_code'].str.slice(0, 10)  # Handle ZIP+4 format
        
        # Validate age (calculate from date_of_birth)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
        df['age'] = (datetime.now() - df['date_of_birth']).dt.days // 365
        df['age_valid'] = (df['age'] >= 18) & (df['age'] <= 120)
        
        # Validate account balance (should be numeric and reasonable)
        df['account_balance'] = pd.to_numeric(df['account_balance'], errors='coerce')
        df['balance_valid'] = (df['account_balance'] >= -50000) & (df['account_balance'] <= 10000000)
        
        # Validate credit score
        df['credit_score'] = pd.to_numeric(df['credit_score'], errors='coerce')
        df['credit_score_valid'] = (df['credit_score'] >= 300) & (df['credit_score'] <= 850)
        
        # Validate annual income
        df['annual_income'] = pd.to_numeric(df['annual_income'], errors='coerce')
        df['income_valid'] = (df['annual_income'] >= 0) & (df['annual_income'] <= 10000000)
        
        # Create data quality score
        quality_columns = ['email_valid', 'phone_valid', 'age_valid', 'balance_valid', 
                          'credit_score_valid', 'income_valid']
        df['data_quality_score'] = df[quality_columns].sum(axis=1) / len(quality_columns)
        
        # Flag customers with low data quality
        df['high_quality'] = df['data_quality_score'] >= 0.8
        
        # Clean text fields
        text_fields = ['first_name', 'last_name', 'city', 'state', 'address']
        for field in text_fields:
            df[field] = df[field].str.strip().str.title()
        
        # Standardize state codes
        df['state'] = df['state'].str.upper()
        
        # Create customer segments based on income and balance
        df['customer_segment'] = pd.cut(df['annual_income'], 
                                       bins=[0, 30000, 60000, 100000, float('inf')],
                                       labels=['Low Income', 'Middle Income', 'High Income', 'Premium'])
        
        # Create account tenure in days
        df['account_open_date'] = pd.to_datetime(df['account_open_date'])
        df['account_tenure_days'] = (datetime.now() - df['account_open_date']).dt.days
        
        logger.info(f"Customer data cleaning complete. Rows: {initial_rows} -> {len(df)}")
        logger.info(f"High quality customers: {df['high_quality'].sum()} ({df['high_quality'].mean()*100:.1f}%)")
        
        return df
    
    def clean_transactions_data(self, transactions_df, customers_df=None):
        """Clean and validate transaction data"""
        logger.info("Starting transaction data cleaning...")
        
        df = transactions_df.copy()
        initial_rows = len(df)
        
        # Remove duplicate transactions
        df = df.drop_duplicates(subset=['transaction_id'], keep='first')
        
        # Convert date and time columns
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        df['transaction_time'] = pd.to_datetime(df['transaction_time'], format='%H:%M:%S').dt.time
        
        # Create datetime column
        df['transaction_datetime'] = pd.to_datetime(
            df['transaction_date'].astype(str) + ' ' + df['transaction_time'].astype(str)
        )
        
        # Validate amounts
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df['amount_valid'] = (~df['amount'].isna()) & (df['amount'].abs() > 0) & (df['amount'].abs() < 100000)
        
        # Clean merchant names
        df['merchant_name'] = df['merchant_name'].str.strip().str.title()
        df['merchant_city'] = df['merchant_city'].str.strip().str.title()
        df['merchant_state'] = df['merchant_state'].str.upper()
        
        # Validate customer IDs exist (if customers_df provided)
        if customers_df is not None:
            valid_customers = set(customers_df['customer_id'])
            df['customer_exists'] = df['customer_id'].isin(valid_customers)
        else:
            df['customer_exists'] = True
        
        # Create derived features
        df['hour'] = df['transaction_datetime'].dt.hour
        df['day_of_week'] = df['transaction_datetime'].dt.dayofweek
        df['month'] = df['transaction_datetime'].dt.month
        df['year'] = df['transaction_datetime'].dt.year
        df['quarter'] = df['transaction_datetime'].dt.quarter
        
        # Business hours flag (9 AM to 6 PM on weekdays)
        df['business_hours'] = (
            (df['hour'] >= 9) & (df['hour'] <= 18) & (df['day_of_week'] < 5)
        )
        
        # Create time-based features
        df['is_night'] = (df['hour'] >= 22) | (df['hour'] <= 6)
        df['is_early_morning'] = (df['hour'] >= 6) & (df['hour'] <= 9)
        df['is_late_night'] = (df['hour'] >= 22) | (df['hour'] <= 2)
        
        # Amount categories
        df['amount_abs'] = df['amount'].abs()
        df['amount_category'] = pd.cut(df['amount_abs'], 
                                     bins=[0, 10, 50, 200, 1000, float('inf')],
                                     labels=['Micro', 'Small', 'Medium', 'Large', 'Very Large'])
        
        # Clean categories
        df['category'] = df['category'].str.strip().str.title()
        
        # Flag potentially suspicious transactions
        df['high_amount'] = df['amount_abs'] > df['amount_abs'].quantile(0.99)
        df['unusual_time'] = df['is_late_night']
        df['weekend_business'] = df['is_weekend'] & df['category'].isin(['Bill Payment', 'Transfer'])
        
        # Create transaction quality score
        quality_columns = ['amount_valid', 'customer_exists']
        df['transaction_quality'] = df[quality_columns].sum(axis=1) / len(quality_columns)
        
        # Remove invalid transactions
        valid_transactions = df[
            (df['amount_valid']) & 
            (df['customer_exists']) & 
            (df['transaction_date'] >= '2020-01-01') &
            (df['transaction_date'] <= datetime.now().date())
        ]
        
        logger.info(f"Transaction data cleaning complete. Rows: {initial_rows} -> {len(valid_transactions)}")
        logger.info(f"Removed {initial_rows - len(valid_transactions)} invalid transactions")
        
        return valid_transactions
    
    def create_aggregated_features(self, transactions_df, customers_df):
        """Create customer-level aggregated features from transactions"""
        logger.info("Creating aggregated customer features...")
        
        # Customer transaction summary
        customer_agg = transactions_df.groupby('customer_id').agg({
            'amount': ['count', 'sum', 'mean', 'std', 'min', 'max'],
            'transaction_date': ['min', 'max'],
            'category': 'nunique',
            'merchant_name': 'nunique',
            'is_fraud': ['sum', 'mean'],
            'is_weekend': 'mean',
            'business_hours': 'mean',
            'amount_abs': ['mean', 'std']
        }).round(2)
        
        # Flatten column names
        customer_agg.columns = ['_'.join(col).strip() for col in customer_agg.columns]
        
        # Rename columns for clarity
        column_mapping = {
            'amount_count': 'total_transactions',
            'amount_sum': 'total_amount',
            'amount_mean': 'avg_transaction_amount',
            'amount_std': 'transaction_amount_std',
            'amount_min': 'min_transaction_amount',
            'amount_max': 'max_transaction_amount',
            'transaction_date_min': 'first_transaction_date',
            'transaction_date_max': 'last_transaction_date',
            'category_nunique': 'unique_categories',
            'merchant_name_nunique': 'unique_merchants',
            'is_fraud_sum': 'fraud_transactions',
            'is_fraud_mean': 'fraud_rate',
            'is_weekend_mean': 'weekend_transaction_rate',
            'business_hours_mean': 'business_hours_rate',
            'amount_abs_mean': 'avg_absolute_amount',
            'amount_abs_std': 'absolute_amount_std'
        }
        
        customer_agg = customer_agg.rename(columns=column_mapping)
        
        # Calculate transaction frequency (transactions per day)
        customer_agg['transaction_span_days'] = (
            customer_agg['last_transaction_date'] - customer_agg['first_transaction_date']
        ).dt.days + 1
        
        customer_agg['transactions_per_day'] = (
            customer_agg['total_transactions'] / customer_agg['transaction_span_days']
        ).round(3)
        
        # Monthly spending patterns
        monthly_spending = transactions_df.groupby(['customer_id', 'month'])['amount'].sum().reset_index()
        monthly_variance = monthly_spending.groupby('customer_id')['amount'].std().round(2)
        customer_agg['monthly_spending_variance'] = monthly_variance
        
        # Category preferences (top category by transaction count)
        top_categories = transactions_df.groupby(['customer_id', 'category']).size().reset_index(name='count')
        top_categories = top_categories.loc[top_categories.groupby('customer_id')['count'].idxmax()]
        top_categories = top_categories.set_index('customer_id')['category']
        customer_agg['preferred_category'] = top_categories
        
        # Merge with customer data
        enhanced_customers = customers_df.merge(customer_agg, left_on='customer_id', right_index=True, how='left')
        
        # Fill NaN values for customers with no transactions
        transaction_columns = customer_agg.columns
        enhanced_customers[transaction_columns] = enhanced_customers[transaction_columns].fillna(0)
        
        logger.info(f"Created {len(transaction_columns)} aggregated features for {len(enhanced_customers)} customers")
        
        return enhanced_customers
    
    def detect_anomalies(self, transactions_df):
        """Detect anomalous transactions using statistical methods"""
        logger.info("Detecting transaction anomalies...")
        
        df = transactions_df.copy()
        
        # Amount-based anomalies (by customer)
        customer_stats = df.groupby('customer_id')['amount_abs'].agg(['mean', 'std']).fillna(0)
        df = df.merge(customer_stats, left_on='customer_id', right_index=True, suffixes=('', '_customer'))
        
        # Z-score for amount (customer-specific)
        df['amount_zscore'] = np.where(
            df['std_customer'] > 0,
            (df['amount_abs'] - df['mean_customer']) / df['std_customer'],
            0
        )
        
        # Time-based anomalies
        df['unusual_hour'] = (df['hour'] < 6) | (df['hour'] > 22)
        
        # Frequency anomalies (multiple transactions in short time)
        df = df.sort_values(['customer_id', 'transaction_datetime'])
        df['time_since_last'] = df.groupby('customer_id')['transaction_datetime'].diff().dt.total_seconds() / 60
        df['rapid_transaction'] = df['time_since_last'] < 5  # Less than 5 minutes
        
        # Location anomalies (different state than customer's home state)
        # This would require customer address data
        
        # Combine anomaly flags
        df['anomaly_score'] = (
            (df['amount_zscore'].abs() > 3).astype(int) * 3 +
            df['unusual_hour'].astype(int) * 1 +
            df['rapid_transaction'].astype(int) * 2 +
            df['high_amount'].astype(int) * 2
        )
        
        df['is_anomaly'] = df['anomaly_score'] >= 3
        
        logger.info(f"Detected {df['is_anomaly'].sum()} anomalous transactions ({df['is_anomaly'].mean()*100:.2f}%)")
        
        return df

def main():
    """Main transformation pipeline"""
    try:
        # Load raw data
        logger.info("Loading raw data...")
        customers_df = pd.read_csv('../data/customers.csv')
        transactions_df = pd.read_csv('../data/transactions.csv')
        
        transformer = DataTransformer()
        
        # Clean customer data
        cleaned_customers = transformer.clean_customers_data(customers_df)
        
        # Clean transaction data
        cleaned_transactions = transformer.clean_transactions_data(transactions_df, cleaned_customers)
        
        # Create aggregated features
        enhanced_customers = transformer.create_aggregated_features(cleaned_transactions, cleaned_customers)
        
        # Detect anomalies
        transactions_with_anomalies = transformer.detect_anomalies(cleaned_transactions)
        
        # Save cleaned data
        logger.info("Saving cleaned data...")
        enhanced_customers.to_csv('../data/customers_cleaned.csv', index=False)
        transactions_with_anomalies.to_csv('../data/transactions_cleaned.csv', index=False)
        
        # Generate data quality report
        quality_report = {
            'customers': {
                'total': len(enhanced_customers),
                'high_quality': enhanced_customers['high_quality'].sum(),
                'avg_quality_score': enhanced_customers['data_quality_score'].mean()
            },
            'transactions': {
                'total': len(transactions_with_anomalies),
                'anomalies': transactions_with_anomalies['is_anomaly'].sum(),
                'fraud': transactions_with_anomalies['is_fraud'].sum()
            }
        }
        
        logger.info("Data Quality Report:")
        for category, stats in quality_report.items():
            logger.info(f"{category.title()}:")
            for metric, value in stats.items():
                logger.info(f"  {metric}: {value}")
        
        logger.info("Data transformation completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in data transformation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
