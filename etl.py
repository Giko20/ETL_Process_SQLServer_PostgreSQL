from sqlalchemy import create_engine
import pandas as pd
import os
import psycopg2
import csv


pwdmssql = os.environ['MSSQLPASS']
pwdpg = os.environ['PGPASS']
uid = "etl"
driver = "ODBC Driver 17 for SQL Server"
server = os.environ['MSSQLservername']
database = "AdventureWorkers"

# extract data from SQL Server
def extract():
    try:        
        engine = create_engine(f'mssql+pyodbc://{uid}:{pwdmssql}@{server}/{database}?driver={driver}')
        # connect and execute the sql query to reach the wanted database and tables
        src_cursor = engine.connect().execution_options(stream_results=True).execute(""" select  t.name as table_name from sys.tables t where t.name = 'advworkersdata'; """)
        # list the all answers given after executing above query
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            df = pd.read_sql_query(f'SELECT * FROM {tbl[0]}', engine)
            load(df, tbl[0])
    except  Exception as e:
        print('Data extract error: ' + str(e))
    finally:
        # src_conn.close()
        engine.dispose()
        
# load data to postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwdpg}@localhost:5432/advworkers') 
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        df.to_sql(f'{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print('Data imported successfuly!')
    except Exception as e:
        print("Data load error: " + str(e))
        
def show_result():   
    conn2 = psycopg2.connect(
        database="advworkers",
        user=uid,
        password=pwdpg,
        host="localhost",
        port='5432'
    )

    conn2.autocommit = True
    cursor = conn2.cursor()

    #fetching all rows
    query = '''SELECT * FROM public.advworkersdata;'''
    cursor.execute(query)
    with open('created_file.csv', 'w') as f:
        for row in cursor.fetchall():
            f.write(str(row) + "\n")
        
    conn2.commit()
    conn2.close()
    print("close connection...")
        

if __name__ == '__main__':
    try:
        extract()
        show_result()
    except Exception as e:
        print("Error while extracting data: " + str(e))