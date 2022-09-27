#import needed libraries
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

#get password from environmnet var
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']

#sql db details
driver = "{ODBC Driver 17 for SQL Server}"
server = "DESKTOP-OL10VP4"
server1 = "127.0.0.1"
database = "AdventureWorksDW2017;"

# Source connection: sql server
src_conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd)

# Destination: Postgres
engine = create_engine(f'postgresql://{uid}:{pwd}@{server1}:5432/AdventureWorks')

source = pd.read_sql_query(""" SELECT top 10
CustomerKey,GeographyKey,CustomerAlternateKey,Title,FirstName,MiddleName,LastName,NameStyle,BirthDate,MaritalStatus
FROM dbo.DimCustomer; """, src_conn)

tbl_name = "stg_IncrementalLoadTest"
source.to_sql(tbl_name, engine, if_exists='replace', index=False)


target = pd.read_sql('Select * from public."stg_IncrementalLoadTest"', engine)
print(target)

# Let's select two additional rows from the source. We have two new records
source = pd.read_sql_query(""" SELECT top 12
CustomerKey,GeographyKey,CustomerAlternateKey,Title,FirstName,MiddleName,LastName,NameStyle,BirthDate,MaritalStatus
FROM dbo.DimCustomer; """, src_conn)

print(source)

# Also update a record. I will update the middle name for customerkey: 11006
source.loc[source.MiddleName =='G', ['MiddleName']] = 'Gina'

target.apply(tuple,1)
source.apply(tuple,1).isin(target.apply(tuple,1))

changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]


modified = changes[changes.CustomerKey.isin(target.CustomerKey)]


inserts = changes[~changes.CustomerKey.isin(target.CustomerKey)]


def update_to_sql(df, table_name, key_name):
    a = []
    table = table_name
    primary_key = key_name
    temp_table = f"{table_name}_temporary_table"
    for col in df.columns:
        if col == primary_key:
            continue
        a.append(f'"{col}"=s."{col}"')
    print(df)
    df.to_sql(temp_table, engine, if_exists='replace', index=False)

    update_stmt_1 = f'UPDATE public."{table}" f '
    update_stmt_2 = "SET "
    update_stmt_3 = ", ".join(a)
    update_stmt_4 = f' FROM public."{table}" t '
    update_stmt_5 = f' INNER JOIN (SELECT * FROM public."{temp_table}") AS s ON s."{primary_key}"=t."{primary_key}" '
    update_stmt_6 = f' Where f."{primary_key}"=s."{primary_key}" '
    update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 +  update_stmt_6 +";"
    print(update_stmt_7)
    with engine.begin() as cnx:
        cnx.execute(update_stmt_7)

update_to_sql(modified, "stg_IncrementalLoadTest", "CustomerKey")
update_to_sql(inserts, "stg_IncrementalLoadTest", "CustomerKey")
changes.to_sql("stg_IncrementalLoadTest",engine,if_exists='append',index=False)

target = pd.read_sql('Select * from public."stg_IncrementalLoadTest"', engine)
print(target)
