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

    # Lista de bancos de dados de sistema para ignorar (não possuem arquivos físicos úteis)
    system_dbs = ["information_schema", "sys", "db_performance"]

    for db in databases:
        if db.name.lower() in system_dbs:
            logger.info(f"Ignorar a base de dados do sistema: {db.name}")
            continue

        tables = spark.catalog.listTables(db.name)
        for t in tables:
            if t.tableType == "VIEW":
                continue

            try:
                # Tentamos o DESCRIBE EXTENDED que é universal para Hive e Iceberg
                # e extraímos a linha que contém o 'Location'
                location_df = spark.sql(f"DESCRIBE EXTENDED {db.name}.{t.name}")

                # O Spark retorna uma tabela com colunas col_name, data_type, comment
                # Procuramos pela linha onde col_name é 'Location'
                location_row = location_df.filter(
                    F.col("col_name") == "Location"
                ).collect()

                if location_row:
                    loc = location_row[0].data_type
                    all_tables.append((db.name, t.name, loc))
                    logger.info(f"Table mapped: {db.name}.{t.name} -> {loc}")
                else:
                    logger.warning(
                        f"Não foi encontrada a localização para a tabela: {db.name}.{t.name}"
                    )

            except Exception as e:
                logger.error(
                    f"Ignorar tabela {db.name}.{t.name} devido a um erro de metadados: {str(e)}"
                )

    schema = ["db_name", "table_name", "location"]
    return spark.createDataFrame(all_tables, schema)


def list_files_distributed(spark, df_catalog):
    """
    Lógica principal de listagem distribuída.
    Utiliza a API do Hadoop FileSystem dentro de um RDD para paralelizar
    o acesso ao storage entre os Executors.
    """
    logger.info("Iniciando listagem física de arquivos no storage (Parallel Scan)...")

    # 1. Extraímos as configurações do Hadoop para um dicionário Python (Pickle-friendly)
    conf_java = spark.sparkContext._jsc.hadoopConfiguration()
    conf_dict = {}
    iterator = conf_java.iterator()
    while iterator.hasNext():
        item = iterator.next()
        conf_dict[item.getKey()] = item.getValue()

    # Fazemos o broadcast do dicionário simples
    conf_broadcast = spark.sparkContext.broadcast(conf_dict)

    locations_rdd = df_catalog.select("db_name", "table_name", "location").rdd

    def process_partition(rows):
        """Função executada dentro de cada Executor do Spark."""
        # Usamos o gateway da JVM que já está disponível no worker
        from pyspark import SparkContext

        # Recuperamos o SparkContext do Worker e a JVM
        sc = SparkContext.getOrCreate()
        # O segredo: Acessar a JVM via _jvm do contexto local do Worker
        jvm = sc._jvm

        # Reconstruímos a Configuration do Hadoop dentro do Worker
        hadoop_conf = jvm.org.apache.hadoop.conf.Configuration()
        for k, v in conf_broadcast.value.items():
            hadoop_conf.set(k, v)

        # Atalhos para as classes Java do Hadoop
        Path = jvm.org.apache.hadoop.fs.Path
        FileSystem = jvm.org.apache.hadoop.fs.FileSystem

        results = []
        for row in rows:
            db, table, loc = row
            if not loc:
                continue  # Ignorar tabelas sem localização definida
            try:
                # Criamos o objeto de caminho e pegamos o sistema de arquivos
                hadoop_path = Path(loc)
                # No Worker, buscamos o FS usando a config reconstruída
                fs = FileSystem.get(hadoop_path.toUri(), hadoop_conf)

                if fs.exists(hadoop_path):
                    files_iter = fs.listFiles(hadoop_path, True)

                    while files_iter.hasNext():
                        f = files_iter.next()
                        size = f.getLen()
                        # Regra dos arquivos pequenos: < 10MB and > 0
                        is_small = 1 if 0 < size < SMALL_FILE_THRESHOLD else 0
                        results.append((db, table, loc, size, is_small))
                    else:
                        results.append((db, table, loc, -2, 0))  # Directory not found

            except Exception as e:
                # Log do erro (O erro de uma tabela não deve parar o mapeamento global)
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
    """
    Consolida as métricas por tabela e armazena-as no formato Iceberg.
    Assegura que a base de dados e a tabela de destino são criadas, caso não existam.
    """
    logger.info("Agregar métricas e calcular percentagens de integridade...")

    # Logic for metrics calculation
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

    # Infrastructure preparation
    try:
        # Extract database name from TARGET_TABLE (e.g., 'sys_monitoring')
        target_db = TARGET_TABLE.split(".")[0]

        logger.info(f"Verificar se a base de dados {target_db} existe...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")

        logger.info(f"Guardar os resultados na tabela {TARGET_TABLE}...")

        # Using saveAsTable instead of save to register the table in the Metastore
        # Iceberg format will handle the schema and metadata automatically
        df_metrics.write.format("iceberg").mode("append").saveAsTable(TARGET_TABLE)

        logger.info("O processo de auditoria foi concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro crítico durante a agregação e salvamento: {str(e)}")
        raise


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
