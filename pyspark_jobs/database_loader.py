import os
from pyspark.sql import DataFrame

def load_to_aurora(df: DataFrame):
    """
    Executes a high-throughput parallel JDBC write into Aurora PostgreSQL
    with executor-level connection capping.
    """
    jdbc_url = os.getenv("AURORA_JDBC_URL", "jdbc:postgresql://aurora-cluster.internal:5432/healthcare")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "secret")
    target_table = os.getenv("TARGET_TABLE", "public.clinical_events")

    # Coalesce partitions to avoid exhausting database connection pools
    coalesced_df = df.coalesce(4)

    coalesced_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", target_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", "5000") \
        .mode("append") \
        .save()