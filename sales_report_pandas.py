from sqlalchemy import create_engine
import pandas as pd
import csv

try:
    # Connect to the SQLite database
    engine = create_engine('sqlite:///Data Engineer_ETL Assignment.db')

    # Read the sales, cutomer and orders table into a Pandas DataFrame
    sales_df = pd.read_sql_table('sales', engine)
    customer_df = pd.read_sql_table('customers', engine)
    orders_df = pd.read_sql_table('orders', engine)

    # Joining customer and sales table on the basis of cutomer_id
    customer_sales_df = pd.merge(customer_df, sales_df, on='customer_id')
    # Filtering age betwween 18 to 35
    filtered_customer_df =  customer_sales_df.where(customer_sales_df['age'].between(18, 35))
    # Removing Nan values
    filtered_customer_df = filtered_customer_df.dropna()
    # Joining orders table with filtered_customer dataframe
    order_customer_df = pd.merge(orders_df, filtered_customer_df, on='sales_id')
    # Replacing Nan values present in quantity column with 0
    order_customer_df['quantity'] = order_customer_df['quantity'].fillna(0)
    # Renaming the column names
    order_customer_df = order_customer_df.rename(columns={'item_id': 'Item', 'customer_id': 'Customer', 'age': 'Age', 'quantity': 'Quantity'})
    print(order_customer_df.head())
    # Adding the quantites for each unique items represented by item_id for the given customers
    sales_report_df = order_customer_df.groupby(['Item', 'Customer']).agg({'Age':'first', 'Quantity': 'sum'})
    # Taking quantities only greater than 0
    sales_report_df = sales_report_df[sales_report_df['Quantity']>0]
    # Convering the age and quantity column values to integer type
    sales_report_df['Age'] = sales_report_df['Age'].astype(int)
    sales_report_df['Quantity'] = sales_report_df['Quantity'].astype(int)
    # Saving the final dataframe as csv
    sales_report_df.to_csv('pandas_result.csv', sep=';')
except Exception as e:
    print(f'Error --- {e}')
finally: 
    engine.dispose()
