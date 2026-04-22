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
# CREATED: 2026-04-08
# LAST MODIFIED: 2026-04-21
# ---------------------------------------------------------------------------------
# VERSION: 2.6
# DESCRIPTION: Lakehouse Health & Metadata Audit for Cloudera Data Engineering.
# Release Notes:
# - v2.6: Refatoração completa para otimização de performance e robustez, incluindo:
#   - Paralelização da Coleta de Metadados;
#   - Otimização do Scan Físico (Hadoop FileSystem);
#   - Redução do Overhead de JVM Gateway
# ---------------------------------------------------------------------------------

import logging, sys, os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# 1. Configuração de Logging (Melhor prática para Verbose no CDE)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- VARIÁVEIS GLOBAIS PADRÃO ---
logger.info(
    "#############################################################################################\n"
    "##### Variáveis globais padrão definidas (podem ser sobrescritas por argumentos do Job) #####\n"
    "##### DEFAULT_SMALL_FILE_SIZE_MB = 5                                                    #####\n"
    "##### TARGET_TABLE = 'sys_monitoring.lakehouse_health_history'                          #####\n"
    "#############################################################################################"
)
DEFAULT_SMALL_FILE_SIZE_MB = 5
TARGET_TABLE = "sys_monitoring.lakehouse_health_history"


def get_schema_path(
    logger: logging.Logger, table_name: str, base_path: str = "/app/mount"
) -> str:
    """
    Obtém o caminho do arquivo de esquema para uma determinada tabela.

    Args:
        logger (logging.Logger): Instância do Logger.
        table_name (str): O nome da tabela.
        base_path (str): O caminho base onde os arquivos de esquema estão armazenados.

    Returns:
        str: O caminho completo para o arquivo de esquema.
    """
    logger.info(f"Obtendo caminho do esquema para a tabela: {table_name}")
    schema_filename = f"schema_{table_name}.json"
    return os.path.join(base_path, schema_filename)


def get_spark_session() -> SparkSession:
    """
    Cria ou obtém a SparkSession configurada com suporte ao Hive e Iceberg.

    Returns:
        SparkSession: Instância ativa da sessão Spark.
    """
    logger.info("Criando SparkSession com suporte ao Hive...")
    return (
        SparkSession.builder.appName("Lakehouse-Metadata-Audit-Production")
        .enableHiveSupport()
        .getOrCreate()
    )


def get_small_file_threshold() -> int:
    """
    Lê o tamanho do arquivo pequeno a partir dos argumentos do Job (CDE).
    Se não informado ou inválido, retorna o padrão de 5MB.

    Returns:
        int: Tamanho limite em MB para arquivos pequenos.
    """
    logger.info(
        "Lendo o tamanho limite para arquivos pequenos a partir dos argumentos do Job..."
    )

    if len(sys.argv) > 1:
        try:
            val = int(sys.argv[1])
            logger.info(f"Variável recebida via argumento: {val}MB")
            return val
        except ValueError:
            logger.warning(
                f"Argumento inválido: {sys.argv[1]}. Usando padrão de {DEFAULT_SMALL_FILE_SIZE_MB}MB"
            )

    logger.info(
        f"Nenhum argumento fornecido. Usando tamanho padrão: {DEFAULT_SMALL_FILE_SIZE_MB}MB"
    )
    return DEFAULT_SMALL_FILE_SIZE_MB


def get_catalog_metadata(spark: SparkSession) -> StructType:
    """
    Coleta metadados detalhados do catálogo, de forma paralela, usando um esquema explícito.

    Args:
        spark (SparkSession): Instância ativa da sessão Spark.

    Returns:
        DataFrame: DataFrame contendo os metadados brutos do catálogo.
    """
    logger.info("Iniciando coleta de metadados do catálogo (HMS)...")

    # 1. Definição do Schema
    catalog_schema = StructType(
        [
            StructField("uuid", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("db_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("table_type", StringType(), True),
            StructField("partitioning_type", StringType(), True),
            StructField("partitioning_cols", StringType(), True),
            StructField("num_rows", StringType(), True),  # Movido para cá
            StructField("location", StringType(), True),
            StructField("metadata_location", StringType(), True),
            StructField("create_time", StringType(), True),
            StructField("last_access", StringType(), True),
        ]
    )

    # 2. Coleta inicial rápida: lista de todas as tabelas no catálogo
    databases = [
        db.name
        for db in spark.catalog.listDatabases()
        if db.name.lower() not in ["information_schema", "sys", "db_performance"]
    ]

    all_tables_list = []
    for db in databases:
        for t in spark.catalog.listTables(db):
            if t.tableType != "VIEW":
                all_tables_list.append((db, t.name, t.tableType))

    # 3. Transformamos a lista em um RDD para paralelizar a busca detalhada de metadados
    tables_rdd = spark.sparkContext.parallelize(all_tables_list).repartition(100)

    def process_table_metadata(partition: iter) -> list:
        """
        Executado nos Workers: Processa cada tabela da partição em paralelo.

        Args:
            partition (iterator): Iterador de tuplas (db_name, table_name, table_type) para cada tabela a ser processada.

        Returns:
            list: Lista de tuplas contendo os metadados extraídos para cada tabela.
        """
        from pyspark.sql import SparkSession

        worker_spark = SparkSession.builder.getOrCreate()
        results = []

        for row in partition:
            db_name, table_name, t_type_catalog = row
            try:
                # Executa o comando SQL pesado no Worker
                desc_df = worker_spark.sql(f"DESCRIBE EXTENDED {db_name}.{table_name}")
                raw_meta = {r["col_name"]: r["data_type"] for r in desc_df.collect()}

                def get_meta(key: str, default=None) -> str:
                    """
                    Extrai um valor específico dos metadados brutos.

                    Args:
                        key (str): Chave a ser buscada.
                        default (str, optional): Valor padrão caso a chave não seja encontrada.

                    Returns:
                        str: O valor associado à chave ou o valor padrão.
                    """
                    return next(
                        (v for k, v in raw_meta.items() if key.lower() in k.lower()),
                        default,
                    )

                # Extração
                loc = get_meta("Location")
                owner = get_meta("Owner")
                create_time = get_meta("Created Time") or get_meta("CreateTime")
                last_access = get_meta("LastAccessTime")
                meta_loc = get_meta("metadata_location")
                num_rows = get_meta("numRows")
                t_type = get_meta("Table Type") or t_type_catalog
                uuid = get_meta("Table UUID") or get_meta("uuid")

                part_type, part_cols = "NONE", None
                if (
                    get_meta("Partition Information")
                    or "# Partition Information" in raw_meta
                ):
                    part_type = "PARTITIONED"
                    p_cols = [
                        k
                        for k, v in raw_meta.items()
                        if k and not k.startswith("#") and "Partition" in k
                    ]
                    part_cols = ", ".join(p_cols) if p_cols else "Verificar no Catálogo"

                elif get_meta("Num Buckets"):
                    part_type = "BUCKETED"
                    part_cols = f"{get_meta('Num Buckets')} buckets"

                results.append(
                    (
                        uuid,
                        owner,
                        db_name,
                        table_name,
                        t_type,
                        part_type,
                        part_cols,
                        str(num_rows) if num_rows else None,
                        loc,
                        meta_loc,
                        create_time,
                        last_access,
                    )
                )
            except Exception:
                continue  # Pula tabelas com erro para não travar o Job

        return results

    # 4. Execução paralela e conversão de volta para DataFrame
    final_rdd = tables_rdd.mapPartitions(process_table_metadata)

    return spark.createDataFrame(final_rdd, schema=catalog_schema)


def list_files_distributed(
    spark: SparkSession, df_catalog, threshold_bytes: int
) -> StructType:
    """
    Realiza a varredura distribuída de arquivos de forma paralela.
    Aplica repartition para garantir que todos os núcleos do cluster trabalhem simultaneamente.

    Args:
        spark (SparkSession): Instância ativa da sessão Spark.
        df_catalog (DataFrame): DataFrame contendo as localizações das tabelas para varredura.
        threshold_bytes (int): Tamanho limite em bytes para classificação de arquivo pequeno.

    Returns:
        DataFrame: Estatísticas físicas por tabela com scan em paralelo.
    """
    logger.info("Iniciando listagem física de arquivos no storage (Parallel Scan)...")

    # 1. Extraímos as configurações do Hadoop para um dicionário Python (Pickle-friendly)
    conf_java = spark.sparkContext._jsc.hadoopConfiguration()
    conf_dict = {item.getKey(): item.getValue() for item in conf_java.iterator()}

    # Fazemos o broadcast do dicionário simples
    conf_broadcast = spark.sparkContext.broadcast(conf_dict)

    # Broadcast do threshold para os workers
    threshold_broadcast = spark.sparkContext.broadcast(threshold_bytes)

    # 2. Paralelização: O segredo da performance está no repartition.
    locations_rdd = df_catalog.select(
        "db_name", "table_name", "location"
    ).rdd.repartition(100)

    def process_partition(rows: iter) -> list:
        """
        Capturamos o Gateway Py4J que o Spark inicializa no Worker automaticamente

        Args:
            rows (iterator): Iterador de linhas contendo (db_name, table_name, location) para cada tabela a ser processada.

        Returns:
            list: Lista de tuplas contendo (db_name, table_name, location, file_size, is_small) para cada arquivo encontrado.
        """
        logger.info("Processando partição de arquivos no Worker...")

        import pyspark
        from pyspark.java_gateway import launch_gateway

        # O Spark já inicializa um gateway Py4J local no worker
        gateway = pyspark.java_gateway.launch_gateway()
        jvm = gateway.jvm

        # Reconstruímos a configuração Hadoop
        hadoop_conf = jvm.org.apache.hadoop.conf.Configuration()
        for k, v in conf_broadcast.value.items():
            hadoop_conf.set(k, v)

        # Agora podemos usar a API Hadoop FileSystem para acessar os arquivos
        Path = jvm.org.apache.hadoop.fs.Path
        FileSystem = jvm.org.apache.hadoop.fs.FileSystem

        # O threshold para arquivos pequenos também é acessado via broadcast
        current_threshold = threshold_broadcast.value

        # Processamos cada linha da partição, listando os arquivos e coletando seus tamanhos
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
                        is_small = 1 if 0 < size < current_threshold else 0
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


def aggregate_and_save(df_raw_files, df_catalog_meta) -> None:
    """
    Consolida as métricas por tabela e armazena-as no formato Iceberg.
    Assegura que a base de dados e a tabela de destino são criadas, caso não existam.

    Args:
        df_raw_files (DataFrame): DataFrame com dados brutos da listagem de arquivos.
        df_catalog_meta (DataFrame): DataFrame com metadados lógicos do catálogo.
    """
    logger.info("Agregando métricas e calcular percentagens de integridade...")

    # 5 partições são suficientes para consolidar os metadados e gerar poucos arquivos físicos.
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    # Agrega os dados físicos por tabela
    df_physical = df_raw_files.groupBy("db_name", "table_name").agg(
        F.count("file_size").alias("total_files_count"),
        F.sum("file_size").alias("total_size_bytes"),
        F.sum("is_small").alias("small_files_count"),
        F.avg("file_size").alias("avg_file_size_bytes"),
    )

    # Define a ordem final desejada das colunas, colocando métricas físicas após os metadados
    final_column_order = [
        "uuid",
        "owner",
        "db_name",
        "table_name",
        "table_type",
        "partitioning_type",
        "partitioning_cols",
        "num_rows",
        "total_files_count",
        "total_size_bytes",
        "small_files_count",
        "avg_file_size_bytes",
        "small_files_pct",
        "location",
        "metadata_location",
        "create_time",
        "last_access",
        "audit_timestamp",
        "audit_date",
    ]

    # Join com os metadados do catálogo coletados no início
    df_final = (
        df_catalog_meta.join(df_physical, ["db_name", "table_name"], "left")
        .withColumn("audit_timestamp", F.current_timestamp())
        .withColumn(
            "small_files_pct",
            F.round((F.col("small_files_count") / F.col("total_files_count")) * 100, 2),
        )
        # COLUNA DE DATA PARA PARTIÇÃO
        .withColumn("audit_date", F.to_date(F.col("audit_timestamp")))
        .select(*final_column_order)
    )

    # Preparação das infraestruturas de destino
    try:
        # Extrair o nome da base de dados de TARGET_TABLE (por exemplo, «sys_monitoring»)
        target_db = TARGET_TABLE.split(".")[0]

        logger.info(f"Verificar se a base de dados {target_db} existe...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")

        logger.info(f"Guardar os resultados na tabela {TARGET_TABLE}...")

        # Utilizar «saveAsTable» em vez de «save» para registar a tabela no Metastore
        # O formato Iceberg irá gerir o esquema e os metadados automaticamente
        df_final.write.format("iceberg").partitionBy("audit_date").mode(
            "append"
        ).saveAsTable(TARGET_TABLE)

        logger.info("O processo de auditoria foi concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro crítico durante a agregação e salvamento: {str(e)}")
        raise


def run_iceberg_maintenance(spark: SparkSession, table_name: str) -> None:
    """
    Realiza a manutenção automática da tabela Iceberg:
    1. Rewrite Data Files: Compacta arquivos pequenos.
    2. Rewrite Manifests: Otimiza o índice de metadados.
    3. Expire Snapshots: Remove histórico físico antigo (30 dias).

    Args:
        spark (SparkSession): Instância ativa da sessão Spark.
        table_name (str): Nome da tabela para manutenção.
    """
    logger.info(f"Iniciando manutenção automática da tabela {table_name}...")

    try:
        # Compactação de arquivos pequenos (Small Files)
        logger.info("- Compactando arquivos de dados (rewrite_data_files)...")
        spark.sql(f"ALTER TABLE {table_name} EXECUTE rewrite_data_files")

        # Otimização de metadados
        logger.info("- Otimizando manifestos de metadados (rewrite_manifests)...")
        spark.sql(f"ALTER TABLE {table_name} EXECUTE rewrite_manifests")

        # Limpeza de histórico físico para economizar storage
        logger.info("- Expirando snapshots antigos (> 30 dias)...")
        spark.sql(
            f"ALTER TABLE {table_name} EXECUTE expire_snapshots(older_than = 'now() - 30d')"
        )

        logger.info("Manutenção finalizada com sucesso.")

    except Exception as e:
        logger.warning(
            f"Aviso: Falha na manutenção automática (pode ser a 1ª execução): {str(e)}"
        )


if __name__ == "__main__":
    start_time = datetime.now()
    logger.info("=== INICIANDO LAKEHOUSE HEALTH AUDIT ===")

    try:
        # Criamos a SparkSession com suporte ao Hive para acessar o catálogo e realizar operações SQL
        spark = get_spark_session()

        # Obter caminho do esquema usando a nova função
        schema_path = get_schema_path(logger, "lakehouse_health_history")
        logger.info(f"Caminho do arquivo de esquema definido: {schema_path}")

        # Define o tamanho dinamicamente
        SMALL_FILE_SIZE_MB = get_small_file_threshold()
        SMALL_FILE_THRESHOLD_BYTES = SMALL_FILE_SIZE_MB * 1024 * 1024

        # Passo 1: Catálogo
        df_meta = get_catalog_metadata(spark)

        # Passo 2: Listagem Física
        df_files = list_files_distributed(spark, df_meta, SMALL_FILE_THRESHOLD_BYTES)

        # Passo 3: Agregação e Escrita
        aggregate_and_save(df_files, df_meta)

        # Passo 4: Manutenção Automática da Tabela Iceberg
        run_iceberg_maintenance(spark, TARGET_TABLE)

        # Log do tempo total de execução para monitoramento de performance
        duration = datetime.now() - start_time
        logger.info(f"=== JOB CONCLUÍDO EM {duration} ===")

    except Exception as e:
        logger.error(f"FALHA CRÍTICA NO JOB: {str(e)}", exc_info=True)
        raise
