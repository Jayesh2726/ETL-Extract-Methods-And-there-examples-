# Full Data Extraction and Loading to PostgreSQL
import pandas as js
import sqlalchemy as sa

username = 'postgres'               # replace with your username
password = '846833'          # replace with your password
host = 'localhost'
port = '5432'
database = 'DWH'  

df = js.read_csv("E:\\ETL Master\\employee_data01.csv")

df ["Full Name"] = df["FirstName"] + " " + df["LastName"]
df ["Full Name"] = df["Full Name"].str.upper()
print(df)

engine = sa.create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
df.to_sql("Employee01",con=engine, index=False, if_exists="fail")