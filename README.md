# sql_data_warehouse
Building a data warehouse with postgresql to facilitate data analytics 

📊 Data Analytics Pipeline with PostgreSQL & Python

🚀 Project Overview

This project displays an end to end data analytics project, from building a data warehouse to generating actionable insights using Python and PostgreSQL. The pipeline:

Loads data from two datasets: source_crm and source_erp.

Stores raw data in the Bronze schema.

Cleans and processes the data, moving it to the Silver schema.

Creates optimized analytical views in the Gold schema.

📂 Project Structure

```
📂 data_analytics_project/
│
├── 📂 datasets/                        # Raw datasets used for the project (ERP and CRM data)
├── 📂 scripts/                         # SQL and Python scripts for ETL and transformations
│── 📂 docs/                            # Documentation & reports
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project
```

🔗 Understanding the Medallion Architecture

This project follows the Medallion Schema, a layered approach to data organization that improves data quality, performance, and accessibility. The three layers are:

🥉 Bronze Schema (Raw Data Storage)

Stores unprocessed data directly from source_crm and source_erp.

Acts as a landing zone for all incoming data.

Data remains in its original format, including duplicates and inconsistencies.

🥈 Silver Schema (Cleaned & Processed Data)

Data is transformed, deduplicated, and standardized.

Ensures consistent naming conventions and corrects errors.

Acts as an intermediate layer for structured querying.

🥇 Gold Schema (Optimized for Analytics)

Stores business-ready data in an easy-to-query format.

Creates aggregated views for reporting and analytics.

Enables faster insights and decision-making.

📜 License

This project is licensed under the MIT License - see the LICENSE file for details.

📩 Contact

For questions, reach out via your-email@example.com or GitHub Issues.



