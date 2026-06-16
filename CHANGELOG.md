# Changelog

Todos os marcos importantes e alterações incrementais deste projeto de Auditoria de Saúde do Lakehouse serão documentados neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/) e este projeto adere ao versionamento semântico.

## [3.0.3] - 2026-06-16
### Corrigido
- Falha na extração de metadados onde o campo `owner` retornava `UNKNOWN`, `table_type` vinha incorretamente como `MANAGED` e `create_time` falhava. O algoritmo de normalização foi reestruturado para limpar quebras de linha textuais, tabulações e remover de forma estrita o caractere oculto `\u00a0` injetado pelo terminal do Metastore da Cloudera.
- Correção na identificação do formato de tabelas Iceberg (`write_format`) através do mapeamento direto da sub-chave `table_type` e extração automática do motor padrão de escrita (`write.format.default`).
- Resolução do mapeamento do caminho físico dos metadados (`metadata_location`) para tabelas Iceberg, localizando dinamicamente o ficheiro `.json` estruturado no dump do Catálogo.

### Adicionado
- Mecanismo de **Métricas de Contingência (Fallback do Catálogo)**: Se a varredura física do sistema de arquivos (S3/HDFS) falhar, retornar nulo ou valores negativos (`-1`, `-2`) devido a barreiras de rede ou caminhos inacessíveis, o pipeline utiliza automaticamente os metadados estatísticos nativos salvos no HMS (`numFiles` e `totalSize`).
- Lógica de cálculo automatizada para saúde de ficheiros baseada em regras de negócio estritas:
  - `avg_file_size_bytes`: Calculado dinamicamente através da divisão matemática entre o tamanho total e o número total de ficheiros (`totalSize / NumFiles`).
  - `small_files_count` / `small_files_pct`: Caso a listagem física não responda, o script avalia se o tamanho médio calculado da tabela é inferior ao limite (*threshold*) parametrizado para determinar o volume de ficheiros pequenos.

### Modificado
- Alteração do tipo de dados da coluna `audit_timestamp` no DataFrame de persistência Iceberg de `StringType` para `TimestampType`, garantindo o armazenamento nativo do carimbo de data/hora oficial do Spark (`YYYY-MM-DD HH:MM:SS.mmmmmm`) em substituição do formato textual simples.

## [3.0.2] - 2026-06-16
### Corrigido
- Erro de parser sintático (`[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXECUTE'`) na função de governança `run_iceberg_maintenance`. A sintaxe administrativa baseada no Impala (`ALTER TABLE ... EXECUTE`) foi substituída pelos procedimentos de chamada nativa do Spark SQL (`CALL spark_catalog.system...`).
- Correção do cálculo interno do timestamp de expiração de snapshots antigos no S3, convertendo o parâmetro dinamicamente para milissegundos Unix aceitos pela API nativa do Iceberg no Spark.

## [3.0.1] - 2026-06-15
### Corrigido
- Falha crítica que causava o desligamento inesperado do `SparkContext` (`Job cancelled because SparkContext was shut down`) devido ao excesso de concorrência de threads disparadas ao Py4J Gateway e ao Hive Metastore.
- Erro de leitura de metadados onde os campos `owner` retornavam `UNKNOWN`, `table_type` retornava `MANAGED`, e estatísticas lógicas de `num_rows` e `write_format` vinham zeradas/vazias.
- Tratamento de caracteres invisíveis especiais (Non-Breaking Spaces `\u00a0`) injetados pelo formatador textual do Metastore da Cloudera.
- Atualização da sintaxe da API `spark.catalog.listColumns` para aceitar strings qualificadas de tabela (`db.table`), eliminando o *FutureWarning* de depreciação introduzido a partir do Spark 3.4+.

### Modificado
- Redimensionamento e balanceamento do ecossistema de concorrência para arquiteturas de tamanho reduzido (1 Executor / 2 Cores), reduzindo o teto do `ThreadPoolExecutor` para 12 workers e ajustando o `repartition` físico de arquivos para 40 partições estáveis.

## [3.0.0] - 2026-04-23
### Adicionado
- Nova coluna `write_format` para rastreamento e identificação do formato físico de escrita das tabelas (`PARQUET`, `ORC`, `AVRO`, `TEXT/CSV`, `ICEBERG`).
- Integração da API `spark.catalog.listColumns` para extração 100% confiável de colunas de partição (`part_cols`) e do tipo de particionamento (`part_type`), incluindo detecção de *Bucketing*.
- Mecanismo de sincronização Driver-Executor utilizando barreira de contagem física para garantir a persistência completa dos dados no S3 antes do encerramento do Job.

### Otimizado
- Reesclonamento da arquitetura de listagem física (`list_files_distributed`) para uso exclusivo de geradores (`yield`), mitigando falhas críticas de estouro de memória RAM nos Workers (`OOMKilled - Exit Code 137`).

## [2.7.4] - 2026-04-21
### Corrigido
- Correção na lógica de extração do `DESCRIBE EXTENDED` que causava deslocamento de colunas e valores `NULL` indesejados nas métricas operacionais.
- Resolução da falha na captura de metadados lógicos complexos como o campo estatístico `numRows`.

### Adicionado
- Nova função de geração de `UUID` determinístico via Hash MD5 para atuar como Chave Primária Lógica estável, permitindo histórico incremental e consistência analítica temporal.

## [2.7.0] - 2026-04-20
### Adicionado
- Implementação de rotinas automáticas de housekeeping e manutenção nativa do Apache Iceberg (`rewrite_data_files` e `expire_snapshots`) executadas ao final de cada pipeline.
- Suporte a múltiplos argumentos CLI dinâmicos no Job do CDE para flexibilizar a parametrização do limite de arquivos pequenos (`MB_LIMIT`) e customização da tabela de destino (`TARGET_TABLE`).
- Paralelização massiva da extração de metadados do catálogo (`get_catalog_metadata`) utilizando `ThreadPoolExecutor` (Multithreading) diretamente no Driver para contornar o erro de serialização do `SparkContext` nos workers.

### Modificado
- Alteração do modo de gravação para `append` e inclusão de particionamento físico diário por `audit_date` para habilitar a análise incremental em dashboards do Cloudera Data Visualization.

## [1.0.0] - 2026-04-08
### Adicionado
- Criação e deploy da versão inicial (MVP) do script de auditoria de saúde do Lakehouse focado no mapeamento do Hive Metastore e identificação de tabelas fragmentadas no S3.