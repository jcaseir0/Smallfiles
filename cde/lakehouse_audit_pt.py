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
# VERSION: 2.7
# DESCRIPTION: Lakehouse Health & Metadata Audit for Cloudera Data Engineering.
# Release Notes:
# - v2.7.3: Correção na lógica de extração atual que estão causando os valores NULL e as falhas nas métricas:
#  .0: Incompatibilidade de Schema (O Desvio de Colunas)
#  .1: Nova Função de Geração de UUID para garantir a unicidade e rastreabilidade de cada tabela auditada.
#  .0: Falha na captura de numRows
#  .0: Métricas Físicas com -1
#  .2: Correção do erro CONTEXT_ONLY_VALID_ON_DRIVER causado pelo uso de SparkSession dentro de funções executadas em Workers (mapPartitions).
#  .3: Adição do argumento 2 para alteração do nome padrão da tabela de destino, permitindo flexibilidade total sem necessidade de editar o código.
#  .3: Correção da coleta de metadados da coluna table_type.
#  .3: Adição de nova coluna chamada write_format para identificar o formato de escrita da tabela (ex: Parquet, ORC, AVRO, CSV).
# - v2.6: Refatoração completa para otimização de performance e escalabilidade - estável.
# - v2.5: Correção de bugs e melhorias na coleta de metadados e adição de manutenção automática da tabela Iceberg.
# - v2.4: Implementação de logging detalhado e tratamento de erros robustoo.
# - v2.3: Otimização da varredura de arquivos usando mapPartitions e broadcast de configurações.
# - v2.2: Adição de argumentos dinâmicos para configuração de tamanho de arquivos pequenos.
# - v2.1: Melhoria na definição de schema para evitar erros de inferência.
# - v2.0: Refatoração completa do código para melhor organização e legibilidade.
# ---------------------------------------------------------------------------------

import logging, sys, os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from concurrent.futures import ThreadPoolExecutor

# 1. Configuração de Logging (Melhor prática para Verbose no CDE)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- VARIÁVEIS GLOBAIS PADRÃO ---
logger.info(
    "Informações importantes sobre as variáveis globais padrão definidas para o Job:"
    "\n#############################################################################################\n"
    "##### Variáveis globais padrão definidas (podem ser sobrescritas por argumentos do Job) #####\n"
    "##### DEFAULT_SMALL_FILE_SIZE_MB = 5                                                    #####\n"
    "##### TARGET_TABLE = 'sys_monitoring.lakehouse_health_history'                          #####\n"
    "##### Para alterar, no campo Arguments do seu Job no CDE, adicione:                     #####\n"
    "##### 1º Argumento: Tamanho do arquivo pequeno (MB)                                     #####\n"
    "##### 2º Argumento: Nome da tabela de destino (banco.tabela)                            #####\n"
    "##### Exemplo 1 (Alterando ambos): 10 analytics.minha_tabela_auditoria                  #####\n"
    "##### Exemplo 2 (Alterando apenas o tamanho do arquivo): 10                             #####\n"
    "##### Exemplo 3 (Alterando o nome da tabela): 5 analytics.health_check_v2               #####\n"
    "#############################################################################################"
)
DEFAULT_SMALL_FILE_SIZE_MB = 5
DEFAULT_TARGET_TABLE = "sys_monitoring.lakehouse_health_history"


def get_job_arguments() -> tuple:
    """
    Captura os argumentos passados ao Job do CDE.

    Args:
        Argumento 1: Tamanho do arquivo pequeno (MB)
        Argumento 2: Nome da tabela de destino (banco.tabela)

    Returns:
        tuple: (size_mb, target_table) onde size_mb é o tamanho limite para arquivos pequenos e target_table é o nome da tabela de destino para os resultados.
    """
    size_mb = DEFAULT_SMALL_FILE_SIZE_MB
    target_table = DEFAULT_TARGET_TABLE

    # Verifica o primeiro argumento (Tamanho MB)
    if len(sys.argv) > 1:
        try:
            size_mb = int(sys.argv[1])
            logger.info(f"Argumento 1 recebido (Tamanho MB): {size_mb}")
        except ValueError:
            logger.warning(f"Argumento 1 inválido. Usando padrão: {size_mb}MB")

    # Verifica o segundo argumento (Nome da Tabela)
    if len(sys.argv) > 2:
        target_table = sys.argv[2]
        logger.info(f"Argumento 2 recebido (Tabela Destino): {target_table}")
    else:
        logger.info(f"Argumento 2 não fornecido. Usando padrão: {target_table}")

    return size_mb, target_table


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


def generate_table_uuid(db_name: str, table_name: str) -> str:
    """
    Gera um UUID determinístico para a tabela baseado no seu nome completo.
    Serve como chave primária para garantir a integridade em consultas.

    Args:
        db_name (str): Nome do banco de dados.
        table_name (str): Nome da tabela.

    Returns:
        str: Um hash MD5 único que identifica a tabela.
    """
    import hashlib

    full_name = f"{db_name.lower()}.{table_name.lower()}"
    return hashlib.md5(full_name.encode()).hexdigest()


def get_catalog_metadata(spark: SparkSession) -> StructType:
    """
    Coleta metadados detalhados do catálogo, de forma paralela, usando Multithreading no Driver.

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
            StructField("write_format", StringType(), True),
            StructField("partitioning_type", StringType(), True),
            StructField("partitioning_cols", StringType(), True),
            StructField("num_rows", StringType(), True),
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

    def fetch_table_details(row: tuple) -> tuple:
        """
        Função executada por threads no Driver para consultar o Metastore.

        Args:
            row (tuple): Tupla contendo (db_name, table_name, t_type_catalog)

        Returns:
            tuple: Tupla com os metadados extraídos para a tabela ou None em caso de falha.
        """
        db_name, table_name, t_type_catalog = row
        try:
            # Consulta individual (Thread-safe no Spark Driver)
            desc_df = spark.sql(f"DESCRIBE EXTENDED {db_name}.{table_name}")
            raw_meta = {
                str(r["col_name"])
                .replace(":", "")
                .strip()
                .lower(): str(r["data_type"])
                .strip()
                for r in desc_df.collect()
                if r["col_name"]
            }

            def find_val(search_key: str) -> str:
                """
                Função para encontrar um valor específico em raw_meta.

                Args:
                    search_key (str): A chave a ser buscada (ex: "owner", "location", "numrows").

                Returns:
                    str: O valor correspondente ou None se não encontrado.
                """
                return next(
                    (v for k, v in raw_meta.items() if search_key.lower() in k), None
                )

            # --- Lógica de UUID determinístico ---
            catalog_uuid = find_val("uuid") or find_val("tableid")
            if not catalog_uuid or catalog_uuid in ["None", "NULL", "N/A"]:
                uuid = generate_table_uuid(db_name, table_name)
            else:
                uuid = catalog_uuid

            # --- LÓGICA HIERÁRQUICA DE TABLE_TYPE ---
            # 1. Valor base do Describe
            base_type = find_val("table type") or t_type_catalog
            final_type = base_type

            # 2. Verificação de sub-chaves nos Table Parameters
            is_iceberg = raw_meta.get("table_type") == "ICEBERG"
            is_trino = "trino_version" in raw_meta

            if is_iceberg:
                final_type = "ICEBERG"
            elif is_trino:
                final_type = "TRINO"

            # --- LÓGICA DE IDENTIFICAÇÃO DO WRITE_FORMAT ---
            input_format = find_val("inputformat") or ""
            serde_lib = find_val("serde library") or ""

            # Inicializamos como desconhecido
            write_format = "UNKNOWN"

            # Hierarquia de Identificação:
            if raw_meta.get("table_type") == "ICEBERG":
                # Coleta o valor da sub-chave write.format.default (ex: parquet, orc)
                write_format = (
                    f"ICEBERG ({raw_meta.get('write.format.default', 'parquet')})"
                )
            elif "trino_version" in raw_meta:
                write_format = "TRINO"
            elif "ParquetInputFormat" in input_format:
                write_format = "PARQUET"
            elif "OrcInputFormat" in input_format:
                write_format = "ORC"
            elif "AvroContainerInputFormat" in input_format:
                write_format = "AVRO"
            elif "LazySimpleSerDe" in serde_lib:
                write_format = "TEXT/CSV"

            # --- Extração de Metadados ---
            owner = find_val("owner") or "UNKNOWN"
            num_rows = raw_meta.get("numrows") or raw_meta.get("num_rows") or "0"
            loc = find_val("location") or "UNKNOWN"
            create_time = find_val("createtime") or find_val("created time")
            last_access = find_val("lastaccesstime") or "UNKNOWN"

            meta_loc = (
                raw_meta.get("metadata_location")
                if "ICEBERG" in str(final_type).lower()
                else "N/A"
            )

            part_type = (
                "PARTITIONED"
                if (
                    "partition information" in str(raw_meta.keys())
                    or find_val("partition column")
                )
                else "NONE"
            )
            part_cols = find_val("partition column") or None

            return (
                str(uuid),
                str(owner),
                str(db_name),
                str(table_name),
                str(final_type).upper(),
                str(write_format),
                str(part_type),
                str(part_cols),
                str(num_rows),
                str(loc),
                str(meta_loc),
                str(create_time),
                str(last_access),
            )
        except Exception:
            return None

    # 3. Execução em paralelo usando ThreadPoolExecutor
    # Usamos 20 threads para não sobrecarregar o Metastore, mas acelerar o I/O de consultas individuais.
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(fetch_table_details, all_tables_list))

    # Filtra falhas e converte para DataFrame
    all_tables_metadata = [r for r in results if r is not None]

    return spark.createDataFrame(all_tables_metadata, schema=catalog_schema)


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
        from datetime import datetime

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
                        m_time_ms = f.getModificationTime()
                        dt_object = datetime.fromtimestamp(m_time_ms / 1000.0)
                        date_str = dt_object.strftime("%Y-%m-%d %H:%M:%S")
                        is_small = 1 if 0 < size < current_threshold else 0
                        yield ((db, table, loc, size, is_small, date_str))
                else:
                    # Caminho não existe
                    results.append((db, table, loc, -2, 0, "1900-01-01 00:00:00"))
            except Exception:
                # Erro de permissão/acesso
                results.append((db, table, loc, -1, 0, "1900-01-01 00:00:00"))
        return results

    # Definição do Schema para o DataFrame de arquivos brutos
    file_schema = StructType(
        [
            StructField("db_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("file_size", LongType(), True),
            StructField("is_small", LongType(), True),
            StructField("s3_last_modified", StringType(), True),
        ]
    )

    # O mapPartitions é mais eficiente que flatMap para inicializar objetos JVM uma única vez por partição
    return locations_rdd.mapPartitions(process_partition).toDF(file_schema)


def aggregate_and_save(df_raw_files, df_catalog_meta, target_table_name: str) -> None:
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
        F.max("s3_last_modified").alias("actual_last_access"),
    )

    # Define a ordem final desejada das colunas, colocando métricas físicas após os metadados
    final_column_order = [
        "uuid",
        "owner",
        "db_name",
        "table_name",
        "table_type",
        "write_format",
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
            "last_access", F.coalesce(F.col("actual_last_access"), F.col("last_access"))
        )
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
        target_db = target_table_name.split(".")[0]

        logger.info(f"Verificar se a base de dados {target_db} existe...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")

        logger.info(f"Guardar os resultados na tabela {target_table_name}...")

        # Utilizar «saveAsTable» em vez de «save» para registar a tabela no Metastore
        # O formato Iceberg irá gerir o esquema e os metadados automaticamente
        df_final.write.format("iceberg").partitionBy("audit_date").mode(
            "append"
        ).saveAsTable(target_table_name)

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
        MB_LIMIT, TARGET_TABLE_NAME = get_job_arguments()
        BYTE_LIMIT = MB_LIMIT * 1024 * 1024

        # Passo 1: Catálogo
        df_meta = get_catalog_metadata(spark)

        # Passo 2: Listagem Física
        df_files = list_files_distributed(spark, df_meta, BYTE_LIMIT)

        # Passo 3: Agregação e Escrita
        aggregate_and_save(df_files, df_meta, TARGET_TABLE_NAME)

        # Passo 4: Manutenção Automática da Tabela Iceberg
        run_iceberg_maintenance(spark, TARGET_TABLE_NAME)

        # Log do tempo total de execução para monitoramento de performance
        duration = datetime.now() - start_time
        logger.info(f"=== JOB CONCLUÍDO EM {duration} ===")

    except Exception as e:
        logger.error(f"FALHA CRÍTICA NO JOB: {str(e)}", exc_info=True)
        raise
