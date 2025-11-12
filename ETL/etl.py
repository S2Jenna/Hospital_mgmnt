import sqlalchemy
from sqlalchemy import create_engine,text
import pandas as pd
import kagglehub
import os
import psycopg2
from datetime import datetime


username = "postgres"        
password = "mypassword"    
host = "localhost"           
port = "5433"             
database = "hospital_db" 

# Load Kaggle Data
path = kagglehub.dataset_download("kanakbaghel/hospital-management-dataset")

print("Path to dataset files:", path)
engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
)


# Read CSV files
files = {
    "patients": pd.read_csv(os.path.join(path, "patients.csv")),
    "doctors": pd.read_csv(os.path.join(path, "doctors.csv")),
    "appointments": pd.read_csv(os.path.join(path, "appointments.csv")),
    "treatments": pd.read_csv(os.path.join(path, "treatments.csv")),
    "billing": pd.read_csv(os.path.join(path, "billing.csv")),
}


# Load files
patients = files["patients"].copy()
doctors = files["doctors"].copy()
appointments = files["appointments"].copy()
treatments = files["treatments"].copy()
billing = files["billing"].copy()


# Basic Cleaning
patients["registration_date"] = pd.to_datetime(patients["registration_date"], errors='coerce')
patients["date_of_birth"] = pd.to_datetime(patients["date_of_birth"], errors='coerce')
appointments["appointment_date"] = pd.to_datetime(appointments["appointment_date"], errors='coerce')
treatments["treatment_date"] = pd.to_datetime(treatments["treatment_date"], errors='coerce')
billing["bill_date"] = pd.to_datetime(billing["bill_date"], errors='coerce')

# Clean IDs
patients["patient_id"] = patients["patient_id"].str.replace('P', '').astype(int)
appointments["appointment_id"] = appointments["appointment_id"].str.replace('A', '').astype(int)
appointments["patient_id"] = appointments["patient_id"].str.replace('P', '').astype(int)
appointments["doctor_id"] = appointments["doctor_id"].str.replace('D', '').astype(int)
doctors["doctor_id"] = doctors["doctor_id"].str.replace('D', '').astype(int)
treatments["treatment_id"] = treatments["treatment_id"].str.replace('T', '').astype(int)
treatments["appointment_id"] = treatments["appointment_id"].str.replace('A', '').astype(int)
billing["bill_id"] = billing["bill_id"].str.replace('B', '').astype(int)
billing["treatment_id"] = billing["treatment_id"].str.replace('T', '').astype(int)
billing["patient_id"] = billing["patient_id"].str.replace('P', '').astype(int)

# Compute age & registration year
patients["age"] = (datetime.now() - patients["date_of_birth"]).dt.days // 365
patients["registration_year"] = patients["registration_date"].dt.year


#---- Staging Tables
# Load Staging Tables
for name, df in files.items():
    df.to_sql(name, engine, schema="staging", if_exists="append", index=False)
    print(f"-----Loaded staging.{name} ({len(df)} rows)")

print("--------- ETL to staging complete! ---------")


#---- Dimension Tables
# Load Dimension Tables
dim_tables_to_load = {
    "dim_patients": patients[["patient_id", "gender", "age", "insurance_provider", "registration_year"]],
    "dim_doctors": doctors[["doctor_id", "specialization", "years_experience", "hospital_branch"]],
    "dim_payment_method": pd.DataFrame({"payment_method": billing["payment_method"].unique()}),
    "dim_treatment_type": pd.DataFrame({"treatment_type": treatments["treatment_type"].unique()}),
    "dim_date": pd.DataFrame({
        "full_date": pd.concat([
            patients["registration_date"], patients["date_of_birth"], appointments["appointment_date"],
            treatments["treatment_date"], billing["bill_date"]
        ]).dropna().drop_duplicates()
    })
}

# Add date attributes
dim_tables_to_load["dim_date"]["day"] = dim_tables_to_load["dim_date"]["full_date"].dt.day
dim_tables_to_load["dim_date"]["month"] = dim_tables_to_load["dim_date"]["full_date"].dt.month
dim_tables_to_load["dim_date"]["month_name"] = dim_tables_to_load["dim_date"]["full_date"].dt.month_name()
dim_tables_to_load["dim_date"]["year"] = dim_tables_to_load["dim_date"]["full_date"].dt.year
dim_tables_to_load["dim_date"]["weekday"] = dim_tables_to_load["dim_date"]["full_date"].dt.day_name()

for table_name, df in dim_tables_to_load.items():
    df.to_sql(table_name, engine, schema="dw", if_exists="append", index=False)
    print(f"-----Loaded dimension table: {table_name}")

# Read back dimension keys
dim_patients = pd.read_sql("SELECT patient_key, patient_id FROM dw.dim_patients", engine)
dim_doctors = pd.read_sql("SELECT doctor_key, doctor_id FROM dw.dim_doctors", engine)
dim_payment = pd.read_sql("SELECT payment_key, payment_method FROM dw.dim_payment_method", engine)
dim_treatment = pd.read_sql("SELECT treatment_type_key, treatment_type FROM dw.dim_treatment_type", engine)
dim_date = pd.read_sql("SELECT date_id, full_date FROM dw.dim_date", engine)


#---- Fact Tables
# Fact Appointments
# Ensure full_date in dim_date is datetime
dim_date['full_date'] = pd.to_datetime(dim_date['full_date'], errors='coerce')

# Ensure appointment_date in appointments is datetime
appointments['appointment_date'] = pd.to_datetime(appointments['appointment_date'], errors='coerce')

# Now merge safely
fact_appointments = (
    appointments
    .merge(dim_patients[['patient_key', 'patient_id']], on='patient_id', how='left')
    .merge(dim_doctors[['doctor_key', 'doctor_id']], on='doctor_id', how='left')
    .merge(dim_date[['date_id', 'full_date']], left_on='appointment_date', right_on='full_date', how='left')
    .drop(columns=['patient_id', 'doctor_id', 'full_date'])
)

fact_appointments = fact_appointments[[
    'appointment_id', 'patient_key', 'doctor_key', 'date_id', 'status', 'reason_for_visit'
]]


# Fact Treatments
fact_treatments = (
    treatments
    .merge(fact_appointments[["appointment_id"]], on="appointment_id", how="left")
    .merge(dim_treatment, on="treatment_type", how="left")
    .merge(dim_date, left_on="treatment_date", right_on="full_date", how="left")
    .drop(columns=["treatment_type", "full_date"])
    .rename(columns={"date_id": "treatment_date_id"})
)[["treatment_id", "appointment_id", "treatment_type_key", "cost", "treatment_date_id"]]

# Fact Billing
fact_billing = (
    billing
    .merge(fact_treatments[["treatment_id"]], on="treatment_id", how="left")
    .merge(dim_patients, on="patient_id", how="left")
    .merge(dim_payment, on="payment_method", how="left")
    .merge(dim_date, left_on="bill_date", right_on="full_date", how="left")
    .drop(columns=["patient_id", "payment_method", "full_date"])
    .rename(columns={"date_id": "bill_date_id"})
)[["bill_id", "treatment_id", "patient_key", "payment_key", "bill_date_id", "amount", "payment_status"]]


# Load Fact Tables
fact_tables = {
    "fact_appointments": fact_appointments,
    "fact_treatments": fact_treatments,
    "fact_billing": fact_billing
}

for table_name, df in fact_tables.items():
    df.to_sql(table_name, engine, schema="dw", if_exists="append", index=False)
    print(f"-----Loaded fact table: {table_name}")


print("--------- ETL pipeline complete! ---------")
