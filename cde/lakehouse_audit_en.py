import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 1. Logging Configuration (Best practice for Verbose output in CDE)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global Configurations
SMALL_FILE_THRESHOLD = 10 * 1024 * 1024  # 10MB in Bytes
TARGET_TABLE = "sys_monitoring.lakehouse_health_history"


def get_spark_session():
    """Initializes the Spark Session with Hive and Iceberg support."""
    return (
        SparkSession.builder.appName("Lakehouse-Health-Audit-Production")
        .enableHiveSupport()
        .getOrCreate()
    )


def get_catalog_metadata(spark):
    """
    Lists all tables and their respective physical locations (S3/ADLS).
    Uses spark.catalog to ensure compatibility with SDX/Ranger.
    """
    logger.info("Starting metadata collection from the catalog (HMS)...")
    databases = spark.catalog.listDatabases()
    all_tables = []

    for db in databases:
        tables = spark.catalog.listTables(db.name)
        for t in tables:
            try:
                # DESCRIBE DETAIL extracts the physical location of the table
                details = spark.sql(f"DESCRIBE DETAIL {db.name}.{t.name}").collect()[0]
                all_tables.append((db.name, t.name, details.location))
                logger.info(f"Table mapped: {db.name}.{t.name}")
            except Exception as e:
                logger.warning(
                    f"Could not get details for table {db.name}.{t.name}: {str(e)}"
                )

    schema = ["db_name", "table_name", "location"]
    return spark.createDataFrame(all_tables, schema)


def list_files_distributed(spark, df_catalog):
    """
    Core distributed listing logic.
    Uses the Hadoop FileSystem API within an RDD to parallelize
    storage access across Executors.
    """
    logger.info("Starting physical file listing in storage (Parallel Scan)...")

    locations_rdd = df_catalog.select("db_name", "table_name", "location").rdd

    def process_partition(rows):
        """Function executed within each Spark Executor."""
        # Access the Hadoop Configuration via PySpark Bridge
        sc_context = SparkSession.builder.getOrCreate().sparkContext
        conf = sc_context._jsc.hadoopConfiguration()
        results = []

        for row in rows:
            db, table, loc = row
            try:
                path = sc_context._gateway.jvm.org.apache.hadoop.fs.Path(loc)
                fs = path.getFileSystem(conf)
                files_iter = fs.listFiles(path, True)  # True = Recursive

                while files_iter.hasNext():
                    f = files_iter.next()
                    size = f.getLen()
                    # Small File Rule: less than 10MB and greater than 0 (ignores metadata/placeholders)
                    is_small = 1 if 0 < size < SMALL_FILE_THRESHOLD else 0
                    results.append((db, table, loc, size, is_small))

            except Exception as e:
                # Log error silently to prevent the entire job from failing
                results.append((db, table, loc, -1, 0))

        return results

    # Define Schema for the raw files DataFrame
    file_schema = StructType(
        [
            StructField("db_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("file_size", LongType(), True),
            StructField("is_small", LongType(), True),
        ]
    )

    # mapPartitions is more efficient than flatMap as it initializes JVM objects once per partition
    return locations_rdd.mapPartitions(process_partition).toDF(file_schema)


def aggregate_and_save(df_raw):
    """Aggregates metrics per table and persists them in Iceberg format."""
    logger.info("Aggregating metrics and calculating health percentages...")

    df_metrics = (
        df_raw.groupBy("db_name", "table_name", "location")
        .agg(
            F.count("file_size").alias("total_files_count"),
            F.sum("file_size").alias("total_size_bytes"),
            F.sum("is_small").alias("small_files_count"),
            F.avg("file_size").alias("avg_file_size_bytes"),
        )
        .withColumn("audit_timestamp", F.current_timestamp())
        .withColumn(
            "small_files_pct",
            F.round((F.col("small_files_count") / F.col("total_files_count")) * 100, 2),
        )
    )

    logger.info(f"Persisting results into table {TARGET_TABLE}...")
    df_metrics.write.format("iceberg").mode("append").save(TARGET_TABLE)
    logger.info("Audit process completed successfully.")


if __name__ == "__main__":
    start_time = datetime.now()
    logger.info("=== STARTING LAKEHOUSE HEALTH AUDIT ===")

    try:
        spark = get_spark_session()

        # Step 1: Catalog Mapping
        df_cat = get_catalog_metadata(spark)

        # Step 2: Distributed Physical Listing
        df_files = list_files_distributed(spark, df_cat)

        # Step 3: Aggregation and Persistence
        aggregate_and_save(df_files)

        duration = datetime.now() - start_time
        logger.info(f"=== JOB COMPLETED IN {duration} ===")

    except Exception as e:
        logger.error(f"CRITICAL JOB FAILURE: {str(e)}", exc_info=True)
        raise
