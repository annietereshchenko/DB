import mysql.connector
from mysql.connector import Error
from datetime import date


def get_db_connection(host_name, user_name, user_password, db_name=None):
    my_connection = None
    try:
        my_connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
    except Error as e:
        print(f'The error {e} occurred')
    return my_connection


def create_db(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
    except Error as e:
        print(f'The error {e} occurred')


connection = get_db_connection('localhost', 'root', '')
create_db_query = 'CREATE DATABASE orders_db'
create_db(connection, create_db_query)

db = get_db_connection('localhost', 'root', '', 'orders_db')
cursor = db.cursor()
create_table_query = 'CREATE TABLE orders (ord_no INT, purch_amt FLOAT, ' \
                     'ord_date DATE, customer_id INT, salesman_id INT)'
cursor.execute(create_table_query)

insert_query = "INSERT INTO orders VALUES (%s, %s, %s, %s, %s)"
values = [
    (70001, 150.5, date(2012, 5, 10), 3005, 5002),
    (70009, 270.65, date(2012, 9, 10), 3001, 5005),
    (70002, 65.26, date(2012, 10, 5), 3002, 5001),
    (70004, 110.5,  date(2012, 8, 17), 3009, 5003),
    (70007, 948.5, date(2012, 9, 10), 3005, 5002),
    (70005, 2400.6, date(2012, 7, 27), 3007, 5001),
    (70008, 5760, date(2012, 9, 10), 3002, 5001),
    (70010, 1983.43, date(2012, 10, 10), 3004, 5006),
    (70003, 2480.4, date(2012, 10, 10), 3009, 5003),
    (70012, 250.45, date(2012, 6, 27), 3008, 5002),
    (70011, 75.29, date(2012, 8, 17), 3003, 5007)

]
cursor.executemany(insert_query, values)
db.commit()
print(cursor.rowcount, "records inserted")

select_orders_by_salesman_query = 'SELECT ord_no, ord_date, purch_amt FROM orders WHERE salesman_id=5002'
cursor.execute(select_orders_by_salesman_query)
print(cursor.fetchall())

select_salesman_query = 'SELECT DISTINCT salesman_id FROM orders'
cursor.execute(select_salesman_query)
print(cursor.fetchall())

select_order_details_query = 'SELECT ord_date, salesman_id, ord_no, purch_amt FROM orders WHERE ord_no=70010'
cursor.execute(select_order_details_query)
print(cursor.fetchall())

select_orders_by_range_query = 'SELECT * FROM orders WHERE ord_no BETWEEN 70001 AND 70007'
cursor.execute(select_orders_by_range_query)
print(cursor.fetchall())

delete_db_query = 'DROP DATABASE orders_db'
cursor.execute(delete_db_query)
