import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Using by spark-submit in Airflow with order args: jdbc_table catalog namespace iceberg_table encrypt_cols
## jdbc_table: jdbc table name (ex: database.schema.table)
## catalog: catalog name (ex: raw_vault)
## namespace: namespace name (ex: raw_vault.default)
## iceberg_table: iceberg table name (ex: raw_vault.default.table)
## encrypt_cols: encrypt columns (seperated by "," comma - ex: col1,col2,col3)


def main():
    jdbc_url = os.environ['JDBC_URL']
    username = os.environ['JDBC_USERNAME']
    password = os.environ['JDBC_PASSWORD']
    driver = os.environ['JDBC_DRIVER']
    encrypt_key = os.environ['ENCRYPT_KEY']
    prefix_encrypt_col_name = os.environ['PREFIX_ENCRYPT_COL_NAME']
    jdbc_table = sys.argv[1]
    catalog = sys.argv[2]
    namespace = sys.argv[3]
    iceberg_table = sys.argv[4]
    encrypt_cols = sys.argv[5]
    encrypt_cols = encrypt_cols.split(",")
    spark = SparkSession.builder.getOrCreate()
    properties = {
        "user": username,
        "password": password,
        "driver": driver
    }
    df = spark.read.jdbc(url=jdbc_url, table=jdbc_table, properties=properties)

    for encrypt_col in encrypt_cols:
        temp_key = "{}_{}".format(prefix_encrypt_col_name, encrypt_col)
        enc_expr = "aes_encrypt({},'{}')".format(encrypt_col, encrypt_key)
        df = df.withColumn(temp_key, F.expr(enc_expr))
    df = df.drop(*encrypt_cols)

    spark.sql("DROP TABLE IF EXISTS {catalog}.{namespace}.{iceberg_table}".format(catalog=catalog, namespace=namespace, iceberg_table=iceberg_table))
    df.writeTo("{catalog}.{namespace}.{iceberg_table}".format(catalog=catalog, namespace=namespace, iceberg_table=iceberg_table)).create()

    spark.stop()

if __name__ == '__main__':
    main()