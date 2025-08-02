"""
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.title("Customer Transaction Insights")

df = pd.read_csv("transactions.csv")

st.subheader("Spend by Category")
category_data = df.groupby("category")["amount"].sum()
st.bar_chart(category_data)

st.subheader("Transaction Timeline")
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp')
st.line_chart(df.set_index('timestamp')['amount'])
"""
