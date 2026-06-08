from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
import os

JDBC_URL = (
    f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}"
    f"/{os.environ['POSTGRES_DB']}"
)
JDBC_PROPS = {
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD'],
    "driver": "org.postgresql.Driver",
}


def get_spark():
    return (SparkSession.builder
            .appName("property-ingest")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
            .getOrCreate())


def load_csv_to_bronze(spark: SparkSession, csv_path: str) -> None:
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(csv_path)
          .withColumn("_loaded_at", F.current_timestamp()))
    (df.write
     .mode("overwrite")
     .jdbc(JDBC_URL, "bronze.property_raw", properties=JDBC_PROPS))
    print(f"Loaded {df.count()} rows into bronze.property_raw")


def load_src_crm_to_bronze(spark: SparkSession, table: str) -> None:
    df = (spark.read
          .jdbc(JDBC_URL, f"src_crm.{table}", properties=JDBC_PROPS)
          .withColumn("_loaded_at", F.current_timestamp()))
    (df.write
     .mode("overwrite")
     .jdbc(JDBC_URL, f"bronze.src_crm_{table}", properties=JDBC_PROPS))
    print(f"Loaded {df.count()} rows into bronze.src_crm_{table}")


if __name__ == "__main__":
    spark = get_spark()
    load_csv_to_bronze(spark, "/data/property_raw.csv")
    load_src_crm_to_bronze(spark, "owners")
    load_src_crm_to_bronze(spark, "agents")
    spark.stop()
    print("Ingestion complete.")
