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
# VERSION: 3.0.0
# DESCRIPTION: Lakehouse Health & Metadata Audit for Cloudera Data Engineering.
#
# NOTE: For complete release history and details, see CHANGELOG.md
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
        - Sem argumentos (Usa padrões)
        - Apenas 1 argumento: Pode ser o tamanho (Número) OU o nome da tabela (Texto)
        - 2 argumentos: [tamanho_mb] [nome_tabela]

    Returns:
        tuple: (size_mb, target_table) onde size_mb é o tamanho limite para arquivos pequenos e target_table é o nome da tabela de destino para os resultados.
    """
    size_mb = DEFAULT_SMALL_FILE_SIZE_MB
    target_table = DEFAULT_TARGET_TABLE

    num_args = len(sys.argv) - 1

    if num_args == 1:
        arg = sys.argv[1].strip()
        # Se for apenas números, o usuário quis mudar o tamanho em MB
        if arg.isdigit():
            size_mb = int(arg)
            logger.info(
                f"Apenas 1 argumento recebido. Definindo tamanho_mb = {size_mb}"
            )
        # Se contiver letras ou ponto, o usuário quis passar apenas a tabela
        else:
            target_table = arg
            logger.info(
                f"Apenas 1 argumento recebido. Definindo tabela_destino = {target_table}"
            )

    elif num_args >= 2:
        # Se passar dois argumentos, assume o padrão posicional: [Tamanho] [Tabela]
        arg1 = sys.argv[1].strip()
        arg2 = sys.argv[2].strip()

        try:
            size_mb = int(arg1)
            logger.info(f"Argumento 1 recebido: tamanho_mb = {size_mb}")
        except ValueError:
            logger.warning(
                f"Argumento 1 '{arg1}' não é um número válido. Mantendo padrão: {size_mb}"
            )

        target_table = arg2
        logger.info(f"Argumento 2 recebido: tabela_destino = {target_table}")

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
    Garante resiliência individual por tabela para evitar que erros de catálogo esvaziem o DataFrame.

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
        try:
            for t in spark.catalog.listTables(db):
                if t.tableType != "VIEW":
                    all_tables_list.append((db, t.name, t.tableType))
        except Exception as e:
            logger.warning(f"Não foi possível listar tabelas do banco {db}: {str(e)}")
            continue

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
            # 1. Busca colunas de partição de forma resiliente via catálogo
            partition_cols_list = []
            bucket_cols_list = []
            try:
                columns = spark.catalog.listColumns(f"`{db_name}`.`{table_name}`")
                partition_cols_list = [col.name for col in columns if col.isPartition]
                bucket_cols_list = [col.name for col in columns if col.isBucket]
            except Exception:
                # Se falhar, continuamos e tentaremos identificar o particionamento via DESCRIBE
                pass

            # 2. Consulta o Describe Extended e constrói um dicionário ultra-normalizado
            desc_df = spark.sql(f"DESCRIBE EXTENDED `{db_name}`.`{table_name}`")
            raw_meta = {}

            for r in desc_df.collect():
                col = str(r["col_name"] or "").strip()
                val = str(r["data_type"] or "").strip()

                # Normaliza caracteres invisíveis (\u00a0) muito comuns no HMS da Cloudera
                col_norm = col.replace("\u00a0", " ").replace(":", "").strip().lower()
                val_norm = val.replace("\u00a0", " ").strip()

                if col_norm:
                    raw_meta[col_norm] = val_norm
                else:
                    # Se col_name estiver em branco, significa que estamos dentro de 'Table Parameters'
                    # Onde a linha vem no formato: "chave     valor" separados por tabulação ou múltiplos espaços
                    parts = [p.strip() for p in val_norm.split("\t") if p.strip()]
                    if len(parts) == 2:
                        raw_meta[parts[0].lower()] = parts[1]
                    elif " " in val_norm:
                        # Fallback se vier separado por múltiplos espaços em vez de tabulação
                        parts = [p.strip() for p in val_norm.split(" ") if p.strip()]
                        if len(parts) >= 2:
                            raw_meta[parts[0].lower()] = " ".join(parts[1:])

            # Função auxiliar para encontrar valores específicos no dicionário de metadados, ignorando casos e espaços
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

            # --- 1. IDENTIFICAÇÃO DE TIPO E FORMATO (LÓGICA HIERÁRQUICA) ---
            all_keys_str = "|".join(raw_meta.keys())
            input_format = find_val("inputformat") or ""
            serde_lib = find_val("serde library") or ""

            # Checa se nos parâmetros há menção explícita a Iceberg
            is_iceberg = (
                raw_meta.get("table_type", "").upper() == "ICEBERG"
                or "iceberg" in serde_lib.lower()
            )
            is_trino = "trino_version" in all_keys_str

            if is_iceberg:
                t_type = "ICEBERG"
                # Coleta o write.format.default mapeado do Table Parameters (Ex: parquet)
                fmt_default = raw_meta.get("write.format.default", "parquet")
                write_format = f"ICEBERG ({fmt_default.upper()})"
            elif is_trino:
                t_type = "TRINO"
                write_format = "TRINO"
            elif "ParquetInputFormat" in input_format:
                t_type = "EXTERNAL"
                write_format = "PARQUET"
            elif "OrcInputFormat" in input_format:
                t_type = "EXTERNAL"
                write_format = "ORC"
            elif "AvroContainerInputFormat" in input_format:
                t_type = "EXTERNAL"
                write_format = "AVRO"
            else:
                # Fallback para o tipo base limpando o \u00a0
                t_type = raw_meta.get("table type") or t_type_catalog or "EXTERNAL"
                write_format = (
                    "TEXT/CSV" if "LazySimpleSerDe" in serde_lib else "UNKNOWN"
                )

            # Limpeza estética do tipo de tabela (Remover _TABLE do final)
            t_type = str(t_type).replace("_TABLE", "").upper()

            # --- 2. EXTRAÇÃO DE METADADOS ADICIONAIS ---
            owner = raw_meta.get("owner") or find_val("owner") or "UNKNOWN"

            # Captura estatísticas reais de linhas (numRows mapeado do Table Parameters)
            num_rows = raw_meta.get("numrows") or raw_meta.get("num_rows") or "0"

            loc = raw_meta.get("location") or find_val("location") or "UNKNOWN"
            create_time = raw_meta.get("createtime") or find_val("create") or "UNKNOWN"
            last_access = (
                raw_meta.get("lastaccesstime") or find_val("lastaccess") or "UNKNOWN"
            )

            # Se for data padrão do Unix (vazia/desconfigurada no cluster), tratamos
            if "1969" in str(last_access) or "1970" in str(last_access):
                last_access = "UNKNOWN (NEVER ACCESSED)"

            meta_loc = (
                raw_meta.get("metadata_location")
                if t_type == "ICEBERG"
                else "NOT AN ICEBERG TABLE"
            )

            # Se o UUID nativo do Iceberg/HMS existir no Parameters, nós usamos
            uuid = (
                raw_meta.get("uuid")
                or find_val("tableid")
                or generate_table_uuid(db_name, table_name)
            )

            # --- 3. PARTICIONAMENTO E BUCKETING ---
            if partition_cols_list and bucket_cols_list:
                part_type = "PARTITIONED_AND_BUCKETED"
                part_cols = f"PART: {','.join(partition_cols_list)} | BUCKET: {','.join(bucket_cols_list)}"
            elif partition_cols_list:
                part_type = "PARTITIONED"
                part_cols = ",".join(partition_cols_list)
            elif bucket_cols_list:
                part_type = "BUCKETED"
                part_cols = ",".join(bucket_cols_list)
            else:
                part_type = "NONE"
                part_cols = "N/A"

            return (
                str(uuid),
                str(owner),
                str(db_name),
                str(table_name),
                str(t_type),
                str(write_format),
                str(part_type),
                str(part_cols),
                str(num_rows),
                str(loc),
                str(meta_loc),
                str(create_time),
                str(last_access),
            )
        except Exception as table_err:
            logger.warning(
                f"Erro ao coletar metadados detalhados de {row[0]}.{row[1]}: {str(table_err)}"
            )
            return None

    # 3. Execução em paralelo usando ThreadPoolExecutor para acelerar a coleta de metadados do catálogo
    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(fetch_table_details, all_tables_list))

    # Filtra falhas e converte para DataFrame
    all_tables_metadata = [r for r in results if r is not None]

    logger.info(
        f"Metadados extraídos com sucesso para {len(all_tables_metadata)} de {len(all_tables_list)} tabelas."
    )

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
    ).rdd.repartition(40)

    def process_partition(rows: iter) -> iter:
        """
        Executado nos Workers: Varre os arquivos transmitindo via yield para economizar memória.

        Args:
            rows (iterator): Iterador de linhas contendo (db_name, table_name, location) para cada tabela a ser processada.
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
                    yield ((db, table, loc, -2, 0, "1900-01-01 00:00:00"))
            except Exception:
                # Erro de permissão/acesso
                yield ((db, table, loc, -1, 0, "1900-01-01 00:00:00"))

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

    # Configurações de estabilidade para escrita em Cloud Storage (S3/ADLS)
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    # Protocolo de commit robusto para evitar leaf node error
    spark.conf.set(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
    )

    # Agrega os dados físicos por tabela
    df_physical = df_raw_files.groupBy("db_name", "table_name").agg(
        F.count("file_size").alias("total_files_count"),
        F.sum("file_size").alias("total_size_bytes"),
        F.sum("is_small").alias("small_files_count"),
        F.avg("file_size").alias("avg_file_size_bytes"),
        F.max("s3_last_modified").alias("actual_last_access"),
    )

    # Join com os metadados e lógica de coalesce para last_access
    df_joined = df_catalog_meta.join(df_physical, ["db_name", "table_name"], "left")

    # Cálculo de métricas com tratamento de divisões por zero e nulos
    df_final = (
        df_joined.withColumn("audit_timestamp", F.current_timestamp())
        .withColumn("audit_date", F.to_date(F.current_timestamp()))
        .withColumn(
            "last_access", F.coalesce(F.col("actual_last_access"), F.col("last_access"))
        )
        .withColumn(
            "small_files_pct",
            F.when(
                F.col("total_files_count") > 0,
                F.round(
                    (F.col("small_files_count") / F.col("total_files_count")) * 100, 2
                ),
            ).otherwise(0.0),
        )
    )

    # Garantimos que os tipos de dados estão corretos para o Iceberg
    df_persistence = df_final.select(
        F.col("uuid").cast("string"),
        F.col("owner").cast("string"),
        F.col("db_name").cast("string"),
        F.col("table_name").cast("string"),
        F.col("table_type").cast("string"),
        F.col("write_format").cast("string"),
        F.col("partitioning_type").cast("string"),
        F.col("partitioning_cols").cast("string"),
        F.col("num_rows").cast("string"),
        F.col("total_files_count").cast("long"),
        F.col("total_size_bytes").cast("long"),
        F.col("small_files_count").cast("long"),
        F.col("avg_file_size_bytes").cast("double"),
        F.col("small_files_pct").cast("double"),
        F.col("location").cast("string"),
        F.col("metadata_location").cast("string"),
        F.col("create_time").cast("string"),
        F.col("last_access").cast("string"),
        F.col("audit_timestamp").cast("timestamp"),
        F.col("audit_date").cast("date"),
    )

    # Preparação das infraestruturas de destino
    try:
        # Extrair o nome da base de dados de TARGET_TABLE (por exemplo, «sys_monitoring»)
        target_db = target_table_name.split(".")[0]

        logger.info(f"Verificar se a base de dados {target_db} existe...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")

        logger.info(f"Iniciando gravação na tabela {target_table_name}...")

        # Escrita Iceberg com mergeSchema para suportar evolução
        df_persistence.write.format("iceberg").partitionBy("audit_date").mode(
            "append"
        ).option("mergeSchema", "true").saveAsTable(target_table_name)

        # Ação que força o Driver a esperar o commit físico no S3 antes de continuar
        logger.info("Sincronizando metadados da tabela...")
        final_count = spark.table(target_table_name).count()

        logger.info(f"Auditoria concluída com sucesso. Registros totais: {final_count}")

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
        table_name = DEFAULT_TARGET_TABLE.split(".")[-1]
        schema_path = get_schema_path(logger, table_name)
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
    finally:
        # O container encerrará organicamente após o fim do script
        pass
