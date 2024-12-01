# Airflow template for data pipeline
# Concept following: ELT with dbt applying Data Vault 2.0
## Extract and Load: Using PySpark to extract data from source and load to staging
## Transform: Using dbt to transform data in warehouse (from staging to raw vault, from raw vault to business vault or from business vault to Dimension schema)

### Further: Apply TextToSQL Agent like (WrenAI, ...) for SQL query for Data analytics.
### SQL engines: Spark SQL, Trino/PrestoDB, Dremio
### Metastore: Hive Metastore
### Storage: Object Storage (S3, ADLS gen2, ...), HDFS
### Table Format: Apache Iceberg