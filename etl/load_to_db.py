#!/usr/bin/env python3
"""
Database Loading Module
Loads cleaned customer and transaction data into PostgreSQL database
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseLoader:
    def __init__(self, db_config=None):
        """Initialize database connection"""
        if db_config is None:
            # Default configuration - can be overridden with environment variables
            self.db_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': os.getenv('DB_PORT', '5432'),
                'database': os.getenv('DB_NAME', 'banking_db'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'password')
            }
        else:
            self.db_config = db_config
        
        self.engine = None
        self.connection = None
    
    def connect(self):
        """Establish database connection"""
        try:
            # Create SQLAlchemy engine
            connection_string = (
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
                f"@{self.db_config['host']}:{self.db_config['port']}"
                f"/{self.db_config['database']}"
            )
            self.engine = create_engine(connection_string)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Database connection established successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return False
    
    def create_tables(self):
        """Create database schema"""
        logger.info("Creating database tables...")
        
        # SQL statements to create tables
        create_tables_sql = """
        -- Create customers table
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(20) PRIMARY KEY,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            phone VARCHAR(20),
            phone_cleaned VARCHAR(10),
            date_of_birth DATE,
            age INTEGER,
            address TEXT,
            city VARCHAR(100),
            state VARCHAR(2),
            zip_code VARCHAR(10),
            account_open_date DATE,
            account_balance DECIMAL(15,2),
            credit_score INTEGER,
            annual
