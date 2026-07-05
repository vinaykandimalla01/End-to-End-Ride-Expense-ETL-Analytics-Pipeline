# Data

This directory contains the raw datasets used by the **End-to-End Uber Ride Expense ETL Analytics Pipeline**.

The project processes Uber Ride receipt emails received after every completed trip. These receipts are stored as **`.eml` (Email Message)** files and act as the primary data source for the ETL pipeline.

---

# Folder Structure

```text
data/
│
├── receipts/
│   ├── trip_0001.eml
│   ├── trip_0002.eml
│   ├── ...
│   └── trip_0017.eml
│
└── README.md
```

---

# Dataset Description

Each `.eml` file represents a single Uber Ride transaction.

The ETL pipeline extracts important business information from these email receipts and converts the unstructured email content into structured datasets suitable for analytics.

Typical information extracted includes:

- Ride Receipt ID
- Customer Name
- Driver Name
- Pickup Location
- Dropoff Location
- Trip Fare
- Booking Fee
- Government Contribution
- Total Amount Charged
- Currency
- Trip Date & Time

---

# Sample Receipt Information

The uploaded sample receipts contain records similar to the following:

| Receipt | Customer | Driver | Pickup | Dropoff | Total |
|---------|----------|--------|---------|----------|--------|
| trip_0013.eml | Priya | Kevin | Airport Road | Airport Road | MX$22.73 |
| trip_0014.eml | Ananya | David | HSR Layout | Koramangala | MX$36.63 |
| trip_0015.eml | Priya | Raj | Indiranagar | Electronic City | MX$38.29 |
| trip_0016.eml | Sneha | Luis | MG Road | Whitefield | MX$17.27 |
| trip_0017.eml | Vikram | Amit | HSR Layout | MG Road | MX$40.29 |

---

# Data Pipeline

The raw receipt files move through the following stages during execution:

```text
Uber Receipt Emails (.eml)
          │
          ▼
Amazon S3 Landing Bucket
          │
          ▼
Apache Airflow ETL Pipeline
          │
          ▼
Email Parsing & Data Extraction
          │
          ▼
Structured CSV Files
          │
          ▼
Amazon Redshift
          │
          ▼
Power BI Dashboards
```

---

# Data Processing

During the ETL workflow, the pipeline performs the following operations:

- Reads raw `.eml` receipt files.
- Extracts ride-related information from the email body.
- Cleans and validates the extracted data.
- Generates structured CSV datasets.
- Uploads processed files to Amazon S3.
- Loads data into Amazon Redshift staging tables.
- Populates dimensional and fact tables for analytics.

---

# Notes

- All receipt files are stored in standard `.eml` email format.
- Each receipt represents one completed Uber Ride.
- The data is automatically parsed by the Apache Airflow ETL pipeline.
- The processed data is stored in Amazon Redshift for analytical reporting and visualization through Microsoft Power BI.