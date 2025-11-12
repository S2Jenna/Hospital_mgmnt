-- PATIENTS (staging)
CREATE TABLE IF NOT EXISTS staging.patients (
    patient_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender CHAR(1),
    date_of_birth DATE,
    contact_number TEXT,
    address TEXT,
    registration_date DATE,
    insurance_provider TEXT,
    insurance_number TEXT,
    email TEXT
);

-- DOCTORS
CREATE TABLE IF NOT EXISTS staging.doctors (
    doctor_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    specialization TEXT,
    phone_number TEXT,
    years_experience INT,
    hospital_branch TEXT,
    email TEXT
);

-- APPOINTMENTS
CREATE TABLE IF NOT EXISTS staging.appointments (
    appointment_id INT PRIMARY KEY,
    patient_id INT,
    doctor_id INT,
    appointment_date DATE,
    appointment_time TIME,
    reason_for_visit TEXT,
    status TEXT
);

-- TREATMENTS
CREATE TABLE IF NOT EXISTS staging.treatments (
    treatment_id INT PRIMARY KEY,
    appointment_id INT,
    treatment_type TEXT,
    description TEXT,
    cost NUMERIC,
    treatment_date DATE
);

-- BILLING
CREATE TABLE IF NOT EXISTS staging.billing (
    bill_id INT PRIMARY KEY,
    patient_id INT,
    treatment_id INT,
    bill_date DATE,
    amount NUMERIC,
    payment_method TEXT,
    payment_status TEXT
);
