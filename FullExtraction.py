# Full Data Extraction and Loading to PostgreSQL
import pandas as js
import sqlalchemy as sa

# Database Connection with PostgreSQL
username = 'Your Username'            
password = 'Your Password'         
host = 'localhost'
port = '5432'
database = 'DWH'  

# Extract the source data
df = js.read_csv("E:\\ETL Master\\employee_data01.csv")
# Transform the data
df ["Full Name"] = df["FirstName"] + " " + df["LastName"]
df ["Full Name"] = df["Full Name"].str.upper()
print(df)

#Load data in PostgreSQL
engine = sa.create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

df.to_sql("Employee01",con=engine, index=False, if_exists="fail")
