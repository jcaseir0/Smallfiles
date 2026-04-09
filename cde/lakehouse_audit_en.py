# ---------------------------------------------------------------------------------
# COPYRIGHT (C) 2026, Cloudera, Inc.
# ALL RIGHTS RESERVED.
#
# THIS SOFTWARE IS LICENSED UNDER THE MIT LICENSE.
# PERMISSION IS HEREBY GRANTED, FREE OF CHARGE, TO ANY PERSON OBTAINING A COPY
# OF THIS SOFTWARE AND ASSOCIATED DOCUMENTATION FILES, TO DEAL IN THE SOFTWARE
# WITHOUT RESTRICTION, INCLUDING WITHOUT LIMITATION THE RIGHTS TO USE, COPY,
# MODIFY, MERGE, PUBLISH, DISTRIBUTE, SUBLICENSE, AND/OR SELL COPIES OF THE
# SOFTWARE, AND TO PERMIT PERSONS TO WHOM THE SOFTWARE IS FURNISHED TO DO SO,
# SUBJECT TO THE FOLLOWING CONDITIONS:
#
# THE ABOVE COPYRIGHT NOTICE AND THIS PERMISSION NOTICE SHALL BE INCLUDED IN ALL
# COPIES OR SUBSTANTIAL PORTIONS OF THE SOFTWARE.
#
# AUTHOR: João Caseiro
# EMAIL: jcaseiro@cloudera.com
# CREATED: 2024-06-01
# LAST MODIFIED: 2024-06-01
# ---------------------------------------------------------------------------------
# VERSION: 1.0
# DESCRIPTION: Lakehouse Health & Metadata Audit for Cloudera Data Engineering.
# ---------------------------------------------------------------------------------

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)

# 1. Logging Configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global Settings
SMALL_FILE_THRESHOLD = 10 * 1024 * 1024  # 10MB
TARGET_TABLE = "sys_monitoring.lakehouse_health_history"


def get_spark_session():
    return (
        SparkSession.builder.appName("Lakehouse-Metadata-Audit-Final")
        .enableHiveSupport()
        .getOrCreate()
    )


def get_catalog_metadata(spark):
    """
    Collects detailed metadata (Owner, Partitioning, Type, UUID)
    by parsing DESCRIBE EXTENDED and spark.catalog.
    """
    logger.info("Starting detailed metadata collection from catalog...")
    databases = spark.catalog.listDatabases()
    all_tables_metadata = []
    system_dbs = ["information_schema", "sys", "db_performance", "information_schema"]

    for db in databases:
        if db.name.lower() in system_dbs:
            continue

        tables = spark.catalog.listTables(db.name)
        for t in tables:
            if t.tableType == "VIEW":
                continue

            try:
                # Get raw text metadata
                desc_df = spark.sql(f"DESCRIBE EXTENDED {db.name}.{t.name}")
                raw_meta = {
                    row["col_name"]: row["data_type"] for row in desc_df.collect()
                }

                def get_meta(key, default=None):
                    return next(
                        (v for k, v in raw_meta.items() if key.lower() in k.lower()),
                        default,
                    )

                # Metadata Extraction
                loc = get_meta("Location")
                owner = get_meta("Owner")
                create_time = get_meta("Created Time") or get_meta("CreateTime")
                last_access = get_meta("LastAccessTime")
                meta_loc = get_meta("metadata_location")
                num_rows = get_meta("numRows")
                t_type = get_meta("Table Type")
                uuid = get_meta("Table UUID") or get_meta("uuid")

                # Partitioning/Bucketing Logic
                part_type = "NONE"
                part_cols = None

                if "# Partition Information" in raw_meta or get_meta(
                    "Partition Information"
                ):
                    part_type = "PARTITIONED"
                    p_cols = [
                        p.name
                        for p in spark.catalog.listColumns(t.name, db.name)
                        if p.isPartition
                    ]
                    part_cols = ", ".join(p_cols) if p_cols else None
                elif "Num Buckets" in raw_meta:
                    part_type = "BUCKETED"
                    buckets = get_meta("Num Buckets")
                    b_cols = get_meta("Bucket Columns")
                    part_cols = f"{buckets} buckets over ({b_cols})"

                all_tables_metadata.append(
                    {
                        "db_name": db.name,
                        "table_name": t.name,
                        "location": loc,
                        "owner": owner,
                        "create_time": create_time,
                        "last_access": last_access,
                        "metadata_location": meta_loc,
                        "num_rows": str(num_rows) if num_rows else None,
                        "table_type": t_type,
                        "uuid": uuid,
                        "partitioning_type": part_type,
                        "partitioning_cols": part_cols,
                    }
                )
            except Exception as e:
                logger.error(f"Error mapping {db.name}.{t.name}: {str(e)}")

    return spark.createDataFrame(all_tables_metadata)


def list_files_distributed(spark, df_catalog):
    """
    Distributed file scan using mapPartitions and JVM Gateway bypass.
    """
    logger.info("Broadcasting Hadoop configuration and starting physical scan...")

    conf_java = spark.sparkContext._jsc.hadoopConfiguration()
    conf_dict = {item.getKey(): item.getValue() for item in conf_java.iterator()}
    conf_broadcast = spark.sparkContext.broadcast(conf_dict)

    locations_rdd = df_catalog.select("db_name", "table_name", "location").rdd

    def process_partition(rows):
        import pyspark

        # Launch internal gateway to bypass SparkContext restrictions on workers
        gateway = pyspark.java_gateway.launch_gateway()
        jvm = gateway.jvm

        hadoop_conf = jvm.org.apache.hadoop.conf.Configuration()
        for k, v in conf_broadcast.value.items():
            hadoop_conf.set(k, v)

        Path = jvm.org.apache.hadoop.fs.Path
        FileSystem = jvm.org.apache.hadoop.fs.FileSystem

        results = []
        for row in rows:
            db, table, loc = row
            if not loc or loc == "None":
                continue

            try:
                h_path = Path(loc)
                fs = FileSystem.get(h_path.toUri(), hadoop_conf)
                if fs.exists(h_path):
                    files_iter = fs.listFiles(h_path, True)
                    while files_iter.hasNext():
                        f = files_iter.next()
                        size = f.getLen()
                        is_small = 1 if 0 < size < SMALL_FILE_THRESHOLD else 0
                        results.append((db, table, size, is_small))
            except Exception:
                results.append((db, table, -1, 0))
        return results

    raw_schema = StructType(
        [
            StructField("db_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("file_size", LongType(), True),
            StructField("is_small", LongType(), True),
        ]
    )

    return locations_rdd.mapPartitions(process_partition).toDF(raw_schema)


def aggregate_and_save(df_raw_files, df_catalog_meta):
    """
    Joins physical stats with catalog metadata and saves as Iceberg.
    """
    logger.info("Aggregating physical data and joining with catalog metadata...")

    df_physical = df_raw_files.groupBy("db_name", "table_name").agg(
        F.count("file_size").alias("total_files_count"),
        F.sum("file_size").alias("total_size_bytes"),
        F.sum("is_small").alias("small_files_count"),
        F.avg("file_size").alias("avg_file_size_bytes"),
    )

    df_final = (
        df_catalog_meta.join(df_physical, ["db_name", "table_name"], "left")
        .withColumn("audit_timestamp", F.current_timestamp())
        .withColumn(
            "small_files_pct",
            F.round((F.col("small_files_count") / F.col("total_files_count")) * 100, 2),
        )
    )

    target_db = TARGET_TABLE.split(".")[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")

    logger.info(f"Saving audit results to {TARGET_TABLE}...")
    df_final.write.format("iceberg").mode("append").saveAsTable(TARGET_TABLE)


if __name__ == "__main__":
    spark = get_spark_session()

    # Execution steps
    df_meta = get_catalog_metadata(spark)
    df_files = list_files_distributed(spark, df_meta)
    aggregate_and_save(df_files, df_meta)

    logger.info("LAKEHOUSE AUDIT COMPLETED SUCCESSFULLY.")
