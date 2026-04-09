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
# VERSION: 2.1
# DESCRIPTION: Lakehouse Health & Metadata Audit for Cloudera Data Engineering.
# ---------------------------------------------------------------------------------

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

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

    # DEFINIÇÃO EXPLÍCITA DO SCHEMA
    catalog_schema = StructType(
        [
            StructField("db_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("create_time", StringType(), True),
            StructField("last_access", StringType(), True),
            StructField("metadata_location", StringType(), True),
            StructField("num_rows", StringType(), True),
            StructField("table_type", StringType(), True),
            StructField("uuid", StringType(), True),
            StructField("partitioning_type", StringType(), True),
            StructField("partitioning_cols", StringType(), True),
        ]
    )

    databases = spark.catalog.listDatabases()
    all_tables_metadata = []

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
                # e extraímos o 'Detailed Table Information'
                desc_df = spark.sql(f"DESCRIBE EXTENDED {db.name}.{t.name}")

                # Transformamos em dicionário para busca rápida (chave -> valor)
                raw_meta = {
                    row["col_name"]: row["data_type"] for row in desc_df.collect()
                }

                # Função auxiliar para buscar chaves ignorando maiúsculas/minúsculas
                def get_meta(key, default=None):
                    return next(
                        (v for k, v in raw_meta.items() if key.lower() in k.lower()),
                        default,
                    )

                # Extração de Campos Básicos
                loc = get_meta("Location")
                owner = get_meta("Owner")
                create_time = get_meta("Created Time") or get_meta("CreateTime")
                last_access = get_meta("LastAccessTime")
                meta_loc = get_meta("metadata_location")
                num_rows = get_meta("numRows")
                t_type = get_meta("Table Type")
                uuid = get_meta("Table UUID") or get_meta("uuid")

                # Lógica de Particionamento / Bucketing
                part_type = "NONE"
                part_cols = None

                # Verificamos se há informações de Partição no Describe
                if (
                    get_meta("Partition Information")
                    or "# Partition Information" in raw_meta
                ):
                    part_type = "PARTITIONED"
                    # O Spark lista as partições abaixo da linha '# Partition Information'
                    # Aqui pegamos as colunas do catálogo diretamente para ser mais preciso
                    p_cols = [
                        p.name
                        for p in spark.catalog.listColumns(t.name, db.name)
                        if p.isPartition
                    ]
                    part_cols = ", ".join(p_cols) if p_cols else None

                elif get_meta("Num Buckets") or "Num Buckets" in raw_meta:
                    part_type = "BUCKETED"
                    buckets = get_meta("Num Buckets")
                    b_cols = get_meta("Bucket Columns")
                    part_cols = f"{buckets} buckets over ({b_cols})"

                all_tables_metadata.append(
                    (
                        db.name,
                        t.name,
                        loc,
                        owner,
                        create_time,
                        last_access,
                        meta_loc,
                        str(num_rows) if num_rows else None,
                        t_type,
                        uuid,
                        part_type,
                        part_cols,
                    )
                )
                logger.info(f"Metadados extraídos: {db.name}.{t.name}")

            except Exception as e:
                logger.error(f"Erro ao mapear {db.name}.{t.name}: {str(e)}")

    # Criando o DataFrame com o schema fornecido explicitamente
    return spark.createDataFrame(all_tables_metadata, schema=catalog_schema)


def list_files_distributed(spark, df_catalog):
    """
    Lógica principal de listagem distribuída.
    Utiliza um dicionário de configuração serializável e
    acede diretamente ao gateway py4j local do Worker.
    """
    logger.info("Iniciando listagem física de arquivos no storage (Parallel Scan)...")

    # 1. Extraímos as configurações do Hadoop para um dicionário Python (Pickle-friendly)
    conf_java = spark.sparkContext._jsc.hadoopConfiguration()
    conf_dict = {item.getKey(): item.getValue() for item in conf_java.iterator()}

    # Fazemos o broadcast do dicionário simples
    conf_broadcast = spark.sparkContext.broadcast(conf_dict)

    locations_rdd = df_catalog.select("db_name", "table_name", "location").rdd

    def process_partition(rows):
        """
        Capturamos o Gateway Py4J que o Spark inicializa no Worker automaticamente
        """
        # Usamos o gateway da JVM que já está disponível no worker
        try:
            from py4j.java_gateway import java_import

            # O Spark expõe a JVM nos workers através desse objeto interno
            import sys

            # Acessamos a JVM local do processo Worker
            # No Spark, o gateway padrão nos workers é acessível via:
            from pyspark.java_gateway import launch_gateway

            # No entanto, em Workers CDE, o método mais seguro é instanciar a Config diretamente
            # via py4j bridge se disponível, ou usar o objeto de sistema.
        except ImportError:
            pass

        # Abordagem robusta para CDE: Reconstrução manual via gateway interno
        import pyspark

        gateway = pyspark.java_gateway.launch_gateway()
        jvm = gateway.jvm

        # Reconstruímos a configuração Hadoop
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
                        results.append((db, table, loc, size, is_small))
                else:
                    results.append((db, table, loc, -2, 0))
            except Exception:
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


def aggregate_and_save(df_raw_files, df_catalog_meta):
    """
    Consolida as métricas por tabela e armazena-as no formato Iceberg.
    Assegura que a base de dados e a tabela de destino são criadas, caso não existam.
    """
    logger.info("Agregar métricas e calcular percentagens de integridade...")

    # Agrega os dados físicos por tabela
    df_physical = df_raw_files.groupBy("db_name", "table_name").agg(
        F.count("file_size").alias("total_files_count"),
        F.sum("file_size").alias("total_size_bytes"),
        F.sum("is_small").alias("small_files_count"),
        F.avg("file_size").alias("avg_file_size_bytes"),
    )

    # Join com os metadados do catálogo coletados no início
    df_final = (
        df_catalog_meta.join(df_physical, ["db_name", "table_name"], "left")
        .withColumn("audit_timestamp", F.current_timestamp())
        .withColumn(
            "small_files_pct",
            F.round((F.col("small_files_count") / F.col("total_files_count")) * 100, 2),
        )
    )

    # Preparação das infraestruturas de destino
    try:
        # Extrair o nome da base de dados de TARGET_TABLE (por exemplo, «sys_monitoring»)
        target_db = TARGET_TABLE.split(".")[0]

        logger.info(f"Verificar se a base de dados {target_db} existe...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")

        logger.info(f"Guardar os resultados na tabela {TARGET_TABLE}...")

        # Using saveAsTable instead of save to register the table in the Metastore
        # Iceberg format will handle the schema and metadata automatically
        df_final.write.format("iceberg").mode("append").saveAsTable(TARGET_TABLE)

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
        df_meta = get_catalog_metadata(spark)

        # Passo 2: Listagem Física
        df_files = list_files_distributed(spark, df_meta)

        # Passo 3: Agregação e Escrita
        aggregate_and_save(df_files, df_meta)

        duration = datetime.now() - start_time
        logger.info(f"=== JOB CONCLUÍDO EM {duration} ===")

    except Exception as e:
        logger.error(f"FALHA CRÍTICA NO JOB: {str(e)}", exc_info=True)
        raise
