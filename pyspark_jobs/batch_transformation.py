import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim, regexp_replace, when

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("AWS-EventDriven-ClinicalDataPipeline") \
        .config("spark.sql.shuffle.partitions", "32") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def transform_batch_data(spark: SparkSession, s3_paths: list):
    """
    Reads batch payloads from S3, executes data hygiene and transformations,
    and returns a clean PySpark DataFrame.
    """
    raw_df = spark.read.option("multiline", "true").json(s3_paths)

    # Validate mandatory primary key fields
    valid_df = raw_df.filter(col("patient_id").isNotNull() & (trim(col("patient_id")) != ""))

    # Clean text data and append ingestion audit metadata
    transformed_df = valid_df \
        .withColumn("patient_id", trim(col("patient_id"))) \
        .withColumn("clinical_note", regexp_replace(col("clinical_note"), r"[^\x00-\x7F]+", "")) \
        .withColumn("status", when(col("status").isNull(), "UNKNOWN").otherwise(col("status"))) \
        .withColumn("processed_timestamp", current_timestamp())

    return transformed_df

if __name__ == "__main__":
    spark = create_spark_session()
    raw_s3_paths = os.getenv("BATCH_S3_PATHS", "").split(",")
    
    if raw_s3_paths and raw_s3_paths[0] != "":
        df = transform_batch_data(spark, raw_s3_paths)
        # Import and execute database load
        from database_loader import load_to_aurora
        load_to_aurora(df)
    
    spark.stop()