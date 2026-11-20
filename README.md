# hospital-data-engineering
End-to-end Healthcare Data Engineering Pipeline using SQL Server, Pandas, PySpark, ETL, and Data Warehousing.
# Hospital Data Engineering Pipeline

### End-to-End Healthcare Data Integration and Analytics

A portfolio Data Engineering project focused on integrating hospital and dental clinic data into a centralized SQL Server Data Warehouse using **Python, Pandas, PySpark, SQL, and ETL pipelines**.

## Project Overview

Healthcare organizations generate data across multiple operational systems and departments. Integrating these data sources enables comprehensive analysis of patient visits, healthcare services, financial transactions, and departmental performance.

This project aims to design and implement an end-to-end data pipeline that extracts data from hospital and dental clinic databases, standardizes and transforms the records, and loads analytical datasets into a centralized Data Warehouse.

The project also compares Pandas and PySpark implementations to explore differences in data processing approaches, scalability, and performance.

**Project Status:** In development

## Business Objectives

The project aims to support the following analytical requirements:

- Integrate patient and service data across hospital and dental clinic systems.
- Identify patients who received services from multiple departments.
- Analyze healthcare service costs by patient and department.
- Calculate visit counts and service utilization over time.
- Compare patient payments and insurance contributions where available.
- Support historical reporting and incremental data updates.

## Data Sources

The planned integration covers the following healthcare departments:

| Department | Data Domain |
|---|---|
| Radiology | Imaging services and related charges |
| Sonography | Ultrasound services and related charges |
| Physiotherapy | Treatment sessions and service costs |
| Emergency | Emergency visits and associated services |
| Dentistry | Dental treatments, visits, and costs |

**Data Privacy:** Real patient records, identifying information, database backups, and production credentials are excluded from this public repository. Public examples will use synthetic or appropriately anonymized data.

## Technology Stack

| Technology | Purpose |
|---|---|
| SQL Server | Operational databases and Data Warehouse |
| Python | Data integration and pipeline development |
| Pandas | Data cleaning, transformation, and validation |
| PySpark / Apache Spark | Distributed data processing and ETL |
| SQL / T-SQL | Data modeling, transformation, and analytical queries |
| JDBC / ODBC | Database connectivity |
| Git / GitHub | Version control and project documentation |
| Power BI | Planned reporting and dashboard development |

## Proposed Architecture

```text
         Operational Data Sources
                    |
       +------------+------------+
       |                         |
   Hospital DB                Dental DB
       |                         |
       +------------+------------+
                    |
                EXTRACT
                    |
            Staging / Validation
                    |
                TRANSFORM
             Pandas / PySpark
                    |
                  LOAD
                    |
          SQL Server Data Warehouse
                    |
          Dimensional Data Model
                    |
             SQL Analytics
                    |
           Power BI Dashboards
```

## Data Warehouse Design

The project will use dimensional modeling principles, beginning with a Star Schema.

### Planned Dimension Tables

- **DimPatient:** Integrated patient attributes and source-system identifiers.
- **DimDate:** Calendar attributes for historical analysis.
- **DimService:** Healthcare service classification and descriptions.

### Planned Fact Tables

- **FactService:** Healthcare service transactions, quantities, and financial measures.

The fact table grain, source-to-target mappings, patient identity reconciliation, and handling of multi-service visits will be finalized after analyzing the source database schemas.

## ETL Development

The planned ETL workflow consists of three main stages.

### 1. Extract

- Connect to SQL Server source databases.
- Extract service, visit, patient, and financial records.
- Validate source schemas and key relationships.

### 2. Transform

- Standardize data types and field names.
- Handle missing values and duplicate records.
- Integrate records from different source systems.
- Apply business rules and calculate analytical measures.
- Generate dimension mappings and fact records.

### 3. Load

- Load dimension tables into the Data Warehouse.
- Resolve surrogate keys.
- Load fact tables.
- Validate record counts, totals, and referential integrity.

## Incremental Data Loading

Future pipeline versions will implement incremental processing to avoid unnecessary full reloads.

Planned topics include:

- Watermark-based extraction
- SQL Server Change Tracking or CDC, where appropriate
- Staging tables and set-based loading
- Insert, update, and delete handling
- Pipeline restartability and idempotency
- Data quality and reconciliation checks

## Repository Structure

```text
hospital-data-engineering/
|-- config/
|-- data/
|   `-- sample/
|-- docs/
|-- notebooks/
|-- src/
|   |-- extract/
|   |-- transform/
|   |-- load/
|   `-- pipelines/
|-- sql/
|   |-- schema/
|   `-- queries/
|-- tests/
|-- .env.example
|-- .gitignore
|-- requirements.txt
|-- README.md
`-- LICENSE
```

## Development Roadmap

- [x] Initialize GitHub repository
- [x] Create initial project directory structure
- [ ] Analyze hospital and dental source schemas
- [ ] Define fact table grain and dimensional model
- [ ] Implement source extraction
- [ ] Build Pandas transformation pipeline
- [ ] Build PySpark transformation pipeline
- [ ] Create Data Warehouse tables
- [ ] Implement initial ETL load
- [ ] Add incremental loading
- [ ] Implement automated data quality tests
- [ ] Compare Pandas and PySpark performance
- [ ] Develop analytical SQL queries
- [ ] Create Power BI dashboards

## Getting Started

Setup instructions, sample data generation, database schema deployment, and pipeline execution commands will be added as the implementation progresses.

## Future Enhancements

Potential enhancements include pipeline orchestration, centralized logging, automated testing, CI/CD, and cloud-based data processing.

## License

See the [LICENSE](LICENSE) file for licensing information.