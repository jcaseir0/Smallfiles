import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 1. Configuração de Logging (Melhor prática para Verbose no CDE)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configurações Globais
SMALL_FILE_THRESHOLD = 10 * 1024 * 1024  # 10MB em Bytes
TARGET_TABLE = "sys_monitoring.lakehouse_health_history"


def get_spark_session():
    """Inicializa a Spark Session com suporte a Hive e Iceberg."""
    return (
        SparkSession.builder.appName("Lakehouse-Health-Audit-Production")
        .enableHiveSupport()
        .getOrCreate()
    )


def get_catalog_metadata(spark):
    """
    Lista todas as tabelas e seus respectivos paths (locations).
    Utiliza o spark.catalog para garantir compatibilidade com SDX.
    """
    logger.info("Iniciando coleta de metadados do catálogo (HMS)...")
    databases = spark.catalog.listDatabases()
    all_tables = []

    for db in databases:
        tables = spark.catalog.listTables(db.name)
        for t in tables:
            try:
                # Describe detail extrai o location físico da tabela (S3/ADLS)
                details = spark.sql(f"DESCRIBE DETAIL {db.name}.{t.name}").collect()[0]
                all_tables.append((db.name, t.name, details.location))
                logger.info(f"Tabela mapeada: {db.name}.{t.name}")
            except Exception as e:
                logger.error(
                    f"Skipping table {db.name}.{t.name} due to missing SerDe/StorageHandler: {str(e)}"
                )
                continue

    schema = ["db_name", "table_name", "location"]
    return spark.createDataFrame(all_tables, schema)


def list_files_distributed(spark, df_catalog):
    """
    Lógica principal de listagem distribuída.
    Utiliza a API do Hadoop FileSystem dentro de um RDD para paralelizar
    o acesso ao storage entre os Executors.
    """
    logger.info("Iniciando listagem física de arquivos no storage (Parallel Scan)...")

    locations_rdd = df_catalog.select("db_name", "table_name", "location").rdd

    def process_partition(rows):
        """Função executada dentro de cada Executor do Spark."""
        # Acesso ao JVM do Hadoop via PySpark Bridge
        sc_context = SparkSession.builder.getOrCreate().sparkContext
        conf = sc_context._jsc.hadoopConfiguration()
        results = []

        for row in rows:
            db, table, loc = row
            try:
                path = sc_context._gateway.jvm.org.apache.hadoop.fs.Path(loc)
                fs = path.getFileSystem(conf)
                files_iter = fs.listFiles(path, True)  # True = Recursivo

                count = 0
                while files_iter.hasNext():
                    f = files_iter.next()
                    size = f.getLen()
                    # Regra de Small File: menor que 10MB e maior que 0 (ignora metadados)
                    is_small = 1 if 0 < size < SMALL_FILE_THRESHOLD else 0
                    results.append((db, table, loc, size, is_small))
                    count += 1

            except Exception as e:
                # Log de erro silencioso para não quebrar o Job inteiro
                results.append((db, table, loc, -1, 0))

        return results

    # Definição do Schema para o DataFrame de arquivos brutos
    file_schema = StructType(
        [
            StructField("db_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("file_size", LongType(), True),
            StructField("is_small", LongType(), True),
        ]
    )

    # O mapPartitions é mais eficiente que flatMap para inicializar objetos JVM uma única vez por partição
    return locations_rdd.mapPartitions(process_partition).toDF(file_schema)


def aggregate_and_save(df_raw):
    """Consolida as métricas por tabela e persiste em formato Iceberg."""
    logger.info("Agregando métricas e calculando percentuais de saúde...")

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

    logger.info(f"Persistindo resultados na tabela {TARGET_TABLE}...")
    df_metrics.write.format("iceberg").mode("append").save(TARGET_TABLE)
    logger.info("Processo de auditoria finalizado com sucesso.")


if __name__ == "__main__":
    start_time = datetime.now()
    logger.info("=== INICIANDO LAKEHOUSE HEALTH AUDIT ===")

    try:
        spark = get_spark_session()

        # Passo 1: Catálogo
        df_cat = get_catalog_metadata(spark)

        # Passo 2: Listagem Física
        df_files = list_files_distributed(spark, df_cat)

        # Passo 3: Agregação e Escrita
        aggregate_and_save(df_files)

        duration = datetime.now() - start_time
        logger.info(f"=== JOB CONCLUÍDO EM {duration} ===")

    except Exception as e:
        logger.error(f"FALHA CRÍTICA NO JOB: {str(e)}", exc_info=True)
        raise
