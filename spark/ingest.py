from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os


def get_jdbc_config():
    url = (
        f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}"
        f"/{os.environ['POSTGRES_DB']}"
    )
    props = {
        "user": os.environ['POSTGRES_USER'],
        "password": os.environ['POSTGRES_PASSWORD'],
        "driver": "org.postgresql.Driver",
    }
    return url, props


def get_spark():
    return (SparkSession.builder
            .appName("property-ingest")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
            .getOrCreate())


def load_csv_to_bronze(spark: SparkSession, csv_path: str) -> None:
    url, props = get_jdbc_config()
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(csv_path)
          .withColumn("_loaded_at", F.current_timestamp()))
    count = df.count()
    (df.write
     .mode("overwrite")
     .jdbc(url, "bronze.property_raw", properties=props))
    print(f"Loaded {count} rows into bronze.property_raw")


def load_src_crm_to_bronze(spark: SparkSession, table: str) -> None:
    url, props = get_jdbc_config()
    df = (spark.read
          .jdbc(url, f"src_crm.{table}", properties=props)
          .withColumn("_loaded_at", F.current_timestamp()))
    count = df.count()
    (df.write
     .mode("overwrite")
     .jdbc(url, f"bronze.src_crm_{table}", properties=props))
    print(f"Loaded {count} rows into bronze.src_crm_{table}")


if __name__ == "__main__":
    spark = get_spark()
    try:
        load_csv_to_bronze(spark, "/data/property_raw.csv")
        load_src_crm_to_bronze(spark, "owners")
        load_src_crm_to_bronze(spark, "agents")
        print("Ingestion complete.")
    finally:
        spark.stop()
