import sqlite3 as sql_engine
import sys
import csv

try: 
    # Establishing connection with database
    con = sql_engine.connect('Data Engineer_ETL Assignment.db')
    # Initializing cursor object to execute SQL queries 
    cur = con.cursor()     

    customer_age_table = cur.execute("SELECT filtered_sales.customer_id, filtered_sales.age, filtered_sales.item_id, CAST(SUM(COALESCE(filtered_sales.quantity, 0)) AS INTEGER) as total_quantity FROM (SELECT *  FROM (SELECT s.sales_id, s.customer_id, c.age FROM sales AS s JOIN customers AS c ON s.customer_id = c.customer_id WHERE c.age BETWEEN 18 AND 35) AS filtered_customers JOIN orders AS o ON filtered_customers.sales_id = o.sales_id) as filtered_sales GROUP BY filtered_sales.customer_id, filtered_sales.item_id HAVING total_quantity > 0  LIMIT 50;")
    # Fetch all rows
    rows = cur.fetchall()

    csv_file_path = 'sql_result.csv'

    # Write the fetched rows to a CSV file with ';' as the delimiter
    with open(csv_file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=';')
        # Write the header
        csv_writer.writerow(['Customer', 'Age', 'Item', 'Quantity'])
        # Write the rows
        csv_writer.writerows(rows)
except Exception as e: 
    if con: 
        con.rollback()
    print(f'Error --- {e}')
    sys.exit(1) 
finally: 
    if con: 
        con.close()  