CREATE MATERIALIZED VIEW IF NOT EXISTS dw.cube_hospital_revenue AS
SELECT 
    d_doctor.specialization,
    d_doctor.hospital_branch,
    d_date.year,
    d_date.month,
    SUM(fb.amount) AS total_revenue,
    COUNT(fb.bill_id) AS bill_count
FROM dw.fact_billing fb
JOIN dw.fact_treatments ft ON fb.treatment_id = ft.treatment_id
JOIN dw.fact_appointments fa ON ft.appointment_id = fa.appointment_id
JOIN dw.dim_doctors d_doctor ON fa.doctor_key = d_doctor.doctor_key
JOIN dw.dim_date d_date ON fb.bill_date_id = d_date.date_id
GROUP BY d_doctor.specialization, d_doctor.hospital_branch, d_date.year, d_date.month;
