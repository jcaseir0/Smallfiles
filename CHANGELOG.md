# Changelog

Todos os marcos importantes e alterações incrementais deste projeto de Auditoria de Saúde do Lakehouse serão documentados neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/) e este projeto adere ao versionamento semântico.

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