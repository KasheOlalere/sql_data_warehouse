import os
import pandas as pd
from sqlalchemy import create_engine, text

user = "postgres"
password = "admin"
host = "localhost"
port = "5432"
database = "warehouse"

engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres')
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

        df.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)

        print(f"crm file: {file} inserted into {schema}.{table_name} table successfully!")

print(f"Database '{database}' has been dropped (if it existed) and recreated successfully!")

sql_queries = """
DROP SCHEMA IF EXISTS bronze;
CREATE SCHEMA bronze;
DROP SCHEMA IF EXISTS silver;
CREATE SCHEMA silver;
DROP SCHEMA IF EXISTS gold;
CREATE SCHEMA gold;
"""

try:
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
    with engine.connect() as conn:
        conn.execute(text(sql_queries))
        conn.commit()  

    print("Schemas successfully created")

except Exception as e:
    print("Error:", e)

finally:
    engine.dispose()  



