import os
import pandas as pd
from sqlalchemy import create_engine

user = "postgres"
password = "admin"
host = "localhost"
port = "5432"
database = "warehouse"
schema = "bronze"  

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

folder_path = "C:/Users/abdul/Desktop/Warehouse/datasets/source_crm"

names = []
# Read CSV files in the crm folder  
for file in os.listdir(folder_path):
    if file.endswith(".csv"): 
        file_path = os.path.join(folder_path, file)
        table_name = 'crm_' + os.path.splitext(file)[0] 
        names.append(table_name)  
        
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.lower()

        df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)

        print(f"crm file: {file} inserted into {schema}.{table_name} table successfully!")

folder_path = "C:/Users/abdul/Desktop/Warehouse/datasets/source_erp"

# Read CSV files in the erp folder  
for file in os.listdir(folder_path):
    if file.endswith(".csv"):  
        file_path = os.path.join(folder_path, file)
        table_name = 'erp_' + os.path.splitext(file)[0].lower()
        names.append(table_name)  
        
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.lower()

        df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)

        print(f"erp file: {file} inserted into {schema}.{table_name} table successfully!")

# Close connection
engine.dispose()


# Confirm all rows were imported
for name in names:
    print(pd.read_sql(f'SELECT COUNT(*) AS count_{name} FROM bronze.{name}',engine))
    print()


