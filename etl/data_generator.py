#!/usr/bin/env python3
"""
Customer Transaction Data Generator
Generates realistic banking transaction data using Faker
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import uuid

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

class BankingDataGenerator:
    def __init__(self):
        self.fake = fake
        self.transaction_categories = [
            'Groceries', 'Gas Station', 'Restaurant', 'Online Shopping',
            'Pharmacy', 'ATM Withdrawal', 'Bill Payment', 'Transfer',
            'Salary Deposit', 'Investment', 'Insurance', 'Entertainment',
            'Travel', 'Healthcare', 'Education', 'Utilities'
        ]
        
        self.merchant_names = {
            'Groceries': ['Walmart', 'Target', 'Kroger', 'Safeway', 'Whole Foods'],
            'Gas Station': ['Shell', 'Exxon', 'BP', 'Chevron', 'Mobil'],
            'Restaurant': ['McDonald\'s', 'Starbucks', 'Subway', 'Pizza Hut', 'KFC'],
            'Online Shopping': ['Amazon', 'eBay', 'Best Buy', 'Apple Store', 'Walmart.com'],
            'Pharmacy': ['CVS', 'Walgreens', 'Rite Aid', 'Pharmacy Plus'],
            'ATM Withdrawal': ['Bank ATM', 'Third Party ATM'],
            'Entertainment': ['Netflix', 'Spotify', 'Movie Theater', 'Concert Venue']
        }

    def generate_customers(self, num_customers=1000):
        """Generate customer data with realistic profiles"""
        customers = []
        
        for i in range(num_customers):
            # Generate customer profile
            age = np.random.normal(45, 15)
            age = max(18, min(85, int(age)))  # Constrain age between 18-85
            
            # Income based on age and education (simplified model)
            base_income = np.random.normal(50000, 20000)
            age_factor = 1 + (age - 30) * 0.02  # Peak earning years
            income = max(25000, base_income * age_factor)
            
            # Credit score based on age and income
            credit_base = 600 + (income - 25000) * 0.002 + (age - 18) * 2
            credit_score = int(max(300, min(850, credit_base + np.random.normal(0, 50))))
            
            customer = {
                'customer_id': f'CUST_{i+1:06d}',
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'date_of_birth': self.fake.date_of_birth(minimum_age=18, maximum_age=85),
                'address': self.fake.street_address(),
                'city': self.fake.city(),
                'state': self.fake.state_abbr(),
                'zip_code': self.fake.zipcode(),
                'account_open_date': self.fake.date_between(start_date='-5y', end_date='today'),
                'account_balance': round(np.random.lognormal(8, 1.5), 2),
                'credit_score': credit_score,
                'annual_income': round(income, 2),
                'employment_status': np.random.choice(['Employed', 'Self-Employed', 'Unemployed', 'Retired'], 
                                                    p=[0.7, 0.15, 0.05, 0.1]),
                'risk_profile': np.random.choice(['Low', 'Medium', 'High'], p=[0.6, 0.3, 0.1])
            }
            customers.append(customer)
        
        return pd.DataFrame(customers)

    def generate_transactions(self, customers_df, num_transactions=50000, 
                            start_date='2023-01-01', end_date='2024-12-31'):
        """Generate realistic transaction data"""
        transactions = []
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Create customer spending profiles
        customer_profiles = {}
        for _, customer in customers_df.iterrows():
            # Base monthly spending on income
            monthly_spending = customer['annual_income'] * 0.06  # 6% of annual income per month
            
            customer_profiles[customer['customer_id']] = {
                'monthly_spending': monthly_spending,
                'preferred_categories': np.random.choice(self.transaction_categories, 
                                                       size=np.random.randint(3, 7), 
                                                       replace=False),
                'fraud_probability': 0.02 if customer['risk_profile'] == 'High' else 0.005
            }
        
        for i in range(num_transactions):
            # Select random customer
            customer = customers_df.sample(1).iloc[0]
            customer_id = customer['customer_id']
            profile = customer_profiles[customer_id]
            
            # Generate transaction date with higher probability for recent dates
            days_range = (end_dt - start_dt).days
            # Weight recent transactions more heavily
            day_offset = int(np.random.beta(0.5, 2) * days_range)
            transaction_date = start_dt + timedelta(days=day_offset)
            
            # Determine if this is a fraudulent transaction
            is_fraud = np.random.random() < profile['fraud_probability']
            
            # Select category (prefer customer's preferred categories)
            if np.random.random() < 0.7 and not is_fraud:
                category = np.random.choice(profile['preferred_categories'])
            else:
                category = np.random.choice(self.transaction_categories)
            
            # Generate amount based on category and customer profile
            if is_fraud:
                # Fraudulent transactions tend to be larger and unusual
                amount = np.random.lognormal(6, 1.5)  # Higher amounts
            else:
                # Normal transaction amounts by category
                category_amounts = {
                    'Groceries': np.random.lognormal(4, 0.5),
                    'Gas Station': np.random.lognormal(3.5, 0.3),
                    'Restaurant': np.random.lognormal(3, 0.7),
                    'Online Shopping': np.random.lognormal(4.5, 1),
                    'ATM Withdrawal': np.random.choice([20, 40, 60, 80, 100, 200]),
                    'Salary Deposit': profile['monthly_spending'] * np.random.uniform(0.8, 1.2),
                    'Bill Payment': np.random.lognormal(5, 0.5),
                }
                amount = category_amounts.get(category, np.random.lognormal(4, 0.8))
            
            # Determine transaction type
            if category in ['Salary Deposit', 'Investment', 'Transfer'] and np.random.random() < 0.8:
                transaction_type = 'Credit'
                amount = abs(amount)
            else:
                transaction_type = 'Debit'
                amount = -abs(amount)
            
            # Select merchant
            if category in self.merchant_names:
                merchant = np.random.choice(self.merchant_names[category])
            else:
                merchant = f"{category} {self.fake.company()}"
            
            # Generate location (sometimes different from customer's city for travel)
            if np.random.random() < 0.1:  # 10% chance of transaction in different city
                city = self.fake.city()
                state = self.fake.state_abbr()
            else:
                city = customer['city']
                state = customer['state']
            
            transaction = {
                'transaction_id': f'TXN_{uuid.uuid4().hex[:12].upper()}',
                'customer_id': customer_id,
                'transaction_date': transaction_date.date(),
                'transaction_time': self.fake.time(),
                'amount': round(amount, 2),
                'transaction_type': transaction_type,
                'category': category,
                'merchant_name': merchant,
                'merchant_city': city,
                'merchant_state': state,
                'payment_method': np.random.choice(['Debit Card', 'Credit Card', 'Cash', 'Check', 'Online Transfer'], 
                                                 p=[0.4, 0.35, 0.1, 0.05, 0.1]),
                'is_weekend': transaction_date.weekday() >= 5,
                'is_fraud': is_fraud,
                'description': f"{merchant} - {category}"
            }
            
            transactions.append(transaction)
        
        return pd.DataFrame(transactions)

def main():
    """Generate and save customer and transaction data"""
    generator = BankingDataGenerator()
    
    print("Generating customer data...")
    customers_df = generator.generate_customers(num_customers=1000)
    
    print("Generating transaction data...")
    transactions_df = generator.generate_transactions(
        customers_df, 
        num_transactions=50000,
        start_date='2023-01-01',
        end_date='2024-12-31'
    )
    
    # Create data directory if it doesn't exist
    import os
    os.makedirs('../data', exist_ok=True)
    
    # Save to CSV files
    print("Saving data to CSV files...")
    customers_df.to_csv('../data/customers.csv', index=False)
    transactions_df.to_csv('../data/transactions.csv', index=False)
    
    print(f"Generated {len(customers_df)} customers and {len(transactions_df)} transactions")
    print(f"Fraud transactions: {transactions_df['is_fraud'].sum()} ({transactions_df['is_fraud'].mean()*100:.2f}%)")
    print("\nData saved to:")
    print("- ../data/customers.csv")
    print("- ../data/transactions.csv")
    
    # Display sample data
    print("\nSample Customer Data:")
    print(customers_df.head())
    print("\nSample Transaction Data:")
    print(transactions_df.head())

if __name__ == "__main__":
    main()
