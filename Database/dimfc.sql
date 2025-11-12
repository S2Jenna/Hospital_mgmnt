DROP SCHEMA IF EXISTS dw CASCADE;
CREATE SCHEMA dw;

------- Dimension Tables
CREATE TABLE dw.dim_patients (
    patient_key SERIAL PRIMARY KEY,
    patient_id INT UNIQUE,
    gender CHAR(1),
    age INT,
    insurance_provider TEXT,
    registration_year INT
);
        
CREATE TABLE dw.dim_doctors (
    doctor_key SERIAL PRIMARY KEY,
    doctor_id INT UNIQUE,
    specialization TEXT,
    years_experience INT,
    hospital_branch TEXT
);
        
CREATE TABLE dw.dim_payment_method (
    payment_key SERIAL PRIMARY KEY,
    payment_method TEXT UNIQUE
);
        
CREATE TABLE dw.dim_treatment_type (
    treatment_type_key SERIAL PRIMARY KEY,
    treatment_type TEXT UNIQUE
);
        
CREATE TABLE dw.dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    day INT,
    month INT,
    month_name TEXT,
    year INT,
    weekday TEXT
);

------- Fact Tables
CREATE TABLE dw.fact_appointments (
    appointment_id INT PRIMARY KEY,
    patient_key INT REFERENCES dw.dim_patients(patient_key),
    doctor_key INT REFERENCES dw.dim_doctors(doctor_key),
    date_id INT REFERENCES dw.dim_date(date_id),
    status TEXT,
    reason_for_visit TEXT
);

CREATE TABLE dw.fact_treatments (
    treatment_id INT PRIMARY KEY,
    appointment_id INT REFERENCES dw.fact_appointments(appointment_id),
    treatment_type_key INT REFERENCES dw.dim_treatment_type(treatment_type_key),
    cost NUMERIC,
    treatment_date_id INT REFERENCES dw.dim_date(date_id)
);

CREATE TABLE dw.fact_billing (
    bill_id INT PRIMARY KEY,
    treatment_id INT REFERENCES dw.fact_treatments(treatment_id),
    patient_key INT REFERENCES dw.dim_patients(patient_key),
    payment_key INT REFERENCES dw.dim_payment_method(payment_key),
    bill_date_id INT REFERENCES dw.dim_date(date_id),
    amount NUMERIC,
    payment_status TEXT
);
