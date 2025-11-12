## 🏥 Hospital Management: Healthcare Data Warehouse & Analytics Dashboard
### 📘 Overview

This project is an end-to-end ETL (Extract, Transform, Load) pipeline and business intelligence project built to simulate a healthcare organization’s data warehouse system. The project integrates data from multiple hospital operations, including patients, doctors, appointments, treatments, and billing — into a unified analytical database.
A Power BI dashboard provides visual insights into hospital revenue, patient demographics, and treatment trends.

### ⚙️ Project Architecture
#### 1. ETL Pipeline (Python + PostgreSQL)
- Extract: Raw operational data (patients, doctors, appointments, billing, treatments) was collected and standardized.
- Transform: Cleaned, validated, and structured data into dimension and fact tables to support OLAP (Online Analytical Processing).
- Load: Data was inserted into a PostgreSQL data warehouse under the dw schema, with an ETL log table tracking job executions, run times, and row counts.

Key ETL Features:
- Modular design for each dimension and fact table (dim_patients, fact_billing, etc.)
- Automated timestamp logging and status reporting via etl_log table.

#### 2. SQL Data Warehouse Design
The data warehouse was modeled using a star schema, optimized for analytical queries and Power BI integration.

##### Dimension Tables:
- ```dim_patients```: Patient demographics (age, gender, region)
- ```dim_doctors```: Doctor information (specialization, experience, branch)
- ```dim_treatment_type```: Categorization of treatments
- ```dim_payment_method```: Payment types (cash, credit, insurance)
- ```dim_date```: Calendar dimension for time-based aggregation

##### Fact Tables:
- ```fact_appointments```: Appointment-level data linked to patients and doctors
- ```fact_treatments```: Details on treatments provided
- ```fact_billing```: Financial transactions and total billed amounts

##### OLAP View:
- ```cube_hospital_revenue```: A materialized view aggregating total revenue and billing count by hospital branch, treatment type, and doctor specialization.
Used for Power BI visualization.

### 📊 Executive Dashboard Insights
<img width="2544" height="1450" alt="image" src="https://github.com/user-attachments/assets/58fde5f8-beef-4219-879a-efd55e87f2ad" />
<img width="2544" height="1412" alt="image" src="https://github.com/user-attachments/assets/a87fd49c-59ed-4402-81f6-d854c9fcc572" />

#### 📈 Key Strategic Takeaways
##### Operational Stability
- Steady revenue between peak months suggests strong patient loyalty and consistent service delivery.
##### Branch Opportunity
- Central Hospital’s leadership in revenue indicates potential best-practice replication for other branches.
##### Digital Payment Growth
- High reliance on card and insurance payments reflects the successful modernization of billing systems.
##### Demographic Insight
- Targeting the 25–34 age segment could further strengthen patient engagement and lifetime value.
##### Talent Retention
- Senior physicians significantly influence revenue; retention and mentorship programs could sustain long-term performance.

