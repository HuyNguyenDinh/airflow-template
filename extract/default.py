import os
import sys
from pyspark.sql import SparkSession


def main():
    jdbc_url = os.environ['JDBC_URL']
    username = os.environ['JDBC_USERNAME']
    password = os.environ['JDBC_PASSWORD']
    driver = os.environ['JDBC_DRIVER']
    jdbc_table = sys.argv[1]
    catalog = sys.argv[2]
    namespace = sys.argv[3]
    iceberg_table = sys.argv[4]
    spark = SparkSession.builder.getOrCreate()
    properties = {
        "user": username,
        "password": password,
        "driver": driver
    }
    df = spark.read.jdbc(url=jdbc_url, table=jdbc_table, properties=properties)
    spark.sql("DROP TABLE IF EXISTS {catalog}.{namespace}.{iceberg_table}".format(catalog=catalog, namespace=namespace, iceberg_table=iceberg_table))
    df.writeTo("{catalog}.{namespace}.{iceberg_table}".format(catalog=catalog, namespace=namespace, iceberg_table=iceberg_table)).create()

    spark.stop()

if __name__ == '__main__':
    main()