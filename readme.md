## Stori Tech Challenge Documentation

---

### Table of Contents
- Project Description
- AWS Resources
- ETL Process and Data Pipeline
- Data Sources
- Python Libraries
- Python Scripts
- Airflow DAG
- Assumptions and Decisions
- Risks and Improvements
- Glossary

---

### Project Description
For this challenge, the objective is to create a simple data pipeline to transfer data from various data sources into a data warehouse. The pipeline is intended to run on a scheduled basis, copying data that has been either inserted or updated in the data sources since the last pipeline execution. The data warehouse is built on a Redshift cluster, and the data sources comprise both a SQL database and a NoSQL database. The exact choice of these databases is left open-ended. The pipeline and related resources must be hosted on AWS.

---

### AWS Resources

1. **Amazon Redshift**:
   - **Description**: Amazon Redshift is a fully managed data warehouse service in the cloud. It allows users to run complex queries and get results in seconds. For this challenge, Redshift serves as the primary data warehouse where consolidated data from various sources is stored.
   - **Configuration**: A small Redshift cluster (e.g., dc2.large) is used, which is cost-effective and can be terminated when not in active use.

2. **Amazon S3 (Simple Storage Service)**:
   - **Description**: Amazon S3 provides object storage through a web interface. It's used to store and retrieve any amount of data at any time, making it a great choice for backup, archiving, and big data analytics.
   - **Usage**: In the ETL scripts, data (like `txns.csv` and `trades.json`) is fetched from an S3 bucket. This indicates that the datasets are stored in an S3 bucket before the ETL process.

3. **Amazon DocumentDB**:
   - **Description**: Amazon DocumentDB is a fully managed document database service that supports MongoDB workloads. It offers high performance, scalability, and availability.
   - **Usage**: The `nosql_collection.py` and `nosql_etl.py` scripts indicate that a DocumentDB named "stori-docdb-2023" is utilized as the NoSQL database source. The "trades" collection within this DocumentDB instance stores the trading data from `trades.json`.

4. **AWS SDK (Boto3)**:
   - **Description**: Boto3 is the Amazon Web Services (AWS) SDK for Python. It allows Python developers to write software that uses services like Amazon S3 and Amazon EC2.
   - **Usage**: Boto3 is employed in the scripts to create sessions with AWS, interact with Amazon S3 to fetch data files, and manage other AWS resources as needed.

---

### Python Libraries

1. **os**: Provides a way of using operating system-dependent functionality, such as reading environment variables.
2. **json**: Used to encode and decode JSON data.
3. **boto3**: The Amazon Web Services (AWS) SDK for Python, enabling Python developers to create, configure, and manage AWS services.
4. **dotenv**: Loads environment variables from a `.env` file.
5. **pandas**: A powerful data manipulation library that provides data structures for efficiently storing large datasets and tools for reshaping, aggregating, and merging data.
6. **psycopg2**: PostgreSQL adapter for Python, facilitating the use of PostgreSQL databases.
7. **pymongo**: A Python driver for MongoDB, allowing interaction with MongoDB databases.
8. **sqlalchemy**: A SQL toolkit and Object-Relational Mapping (ORM) library for Python, allowing the application to communicate with SQL databases.

---

### Python Scripts

- **sql_table.py**: Sets up a PostgreSQL database named "stori" and establishes tables based on the structure defined in the `transactions.sql` file.
- **sql_etl.py**: Extracts transaction data from an S3 bucket CSV file (`txns.csv`), processes and transforms this data, and then loads it into the "transactions" table in the "stori" PostgreSQL database.
- **nosql_collection.py**: Validates the existence of a DocumentDB instance named "stori-docdb-2023" and sets up a collection named "trades" with a predefined schema.
- **nosql_etl.py**: Extracts trade data from an S3 bucket JSON file (`trades.json`), validates this data against the schema of the "trades" collection in DocumentDB, and either updates or inserts the data into the collection.
- **dw_consolidation.py**: Consolidates data from the PostgreSQL "transactions" table and the DocumentDB "trades" collection, and then loads this data into an Amazon Redshift data warehouse.
- **dag.py**: Defines an Apache Airflow DAG to automate the execution of the aforementioned scripts. The DAG installs necessary dependencies and runs the scripts in the sequence of `sql_table.py`, `sql_etl.py`, `nosql_collection.py`, `nosql_etl.py`, and `dw_consolidation.py`.

---

### Airflow DAG

The pipeline's orchestration is handled using Apache Airflow. The defined DAG (`dag.py`) automates the tasks of:
1. Installing necessary Python dependencies.
2. Running the scripts that set up the data sources, perform ETL operations, and consolidate data into Redshift.

The task dependencies ensure that each script runs in the correct order, ensuring a smooth data flow from source to warehouse.

---

### Assumptions and Decisions

- Data in the source databases is either new or updated since the last pipeline run.
- Any encountered errors or mismatches in data schemas will result in the rejection of the specific data record, but the ETL process will continue.
- The pipeline uses AWS resources, incurring costs; therefore, resources like Redshift clusters should be shut down when not in use to save costs.

---

### Risks and Improvements

- **Risks**: Without proper error handling, the pipeline might miss out on critical data or introduce inconsistencies in the Redshift data warehouse.
- **Improvements**: Implementing logging and monitoring can help in tracking the pipeline's performance and catching any anomalies. Additionally, data validation and quality checks can be introduced to ensure the consistency and reliability of the data loaded into Redshift.

---

### Glossary

- **ETL**: Extract, Transform, Load. Refers to the process of extracting data from source databases, transforming it into a warehouse-friendly format, and loading it into a data warehouse.
- **DAG**: Directed Acyclic Graph. In the context of Airflow, it refers to a collection of tasks defining how they run in relation to each other.

---
