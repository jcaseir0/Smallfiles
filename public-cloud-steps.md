# 1. O Script PySpark (`lakehouse_audit_pt.py`)

Este script utiliza processamento distribuído para mapear o catálogo do Hive e listar fisicamente os arquivos no S3/ADLS.

### Principais Funcionalidades:
* **Mapeamento de Catálogo:** Varre o Hive Metastore em busca de tabelas.
* **Listagem Paralela:** Usa a API Hadoop FileSystem nos Executors para rapidez.
* **Resiliência:** Tratamento de erros para tabelas com SerDes ausentes.
* **Persistência Iceberg:** Armazena resultados com histórico (Time Travel).

---

# 2. Preparação de Dependências (Resources)

Para evitar erros de `ClassNotFoundException` detectados anteriormente, precisamos empacotar os JARs necessários.

1.  **Colete os JARs** de um nó de borda do seu cluster:
    * `hive-jdbc-handler.jar`
    * `tez-api.jar`
    * `hive-exec.jar`
2.  **Crie um Resource no CDE:**
    * Vá em **Resources** > **Create Resource**.
    * Nome: `audit-dependency-jars`.
    * Tipo: `Files`.
    * Faça o upload dos arquivos `.jar`.

---

# 3. Criação do Job no CDE

Existem duas formas de criar o Job: via Interface Web (UI) ou via CLI.

### Via Interface Web (UI):
1.  Vá em **Jobs** > **Create Job**.
2.  **Job Type:** `Spark`.
3.  **Application File:** Faça o upload do `lakehouse_audit.py`.
4.  **Arguments:** Adicione os JARs do Resource criado:
    * `--jars /app/mount/audit-dependency-jars/tez-api.jar,/app/mount/audit-dependency-jars/hive-jdbc-handler.jar`
5.  **Configurations:**
    * Em **Mounts**, selecione o Resource `audit-dependency-jars`.
    * Em **Spark Configs**, adicione:
        * `spark.sql.iceberg.handle-timestamp-without-timezone`: `true`
        * `spark.executor.memory`: `4g` (recomendado para scan de arquivos).

---

# 4. Agendamento e Execução

Para garantir que a auditoria seja recorrente, utilizamos o agendador nativo do CDE ou o Airflow.

### Exemplo de Definição do Job (YAML para CLI):
Se preferir automatizar a criação via CLI do CDE:

```yaml
name: lakehouse-health-audit-job
type: spark
mounts:
  - resourceName: audit-dependency-jars
spark:
  file: lakehouse_audit.py
  jars: 
    - /app/mount/audit-dependency-jars/tez-api.jar
    - /app/mount/audit-dependency-jars/hive-jdbc-handler.jar
    - /app/mount/audit-dependency-jars/hive-exec.jar
conf:
  spark.executor.memory: "4g"
  spark.driver.memory: "2g"
schedule:
  cron: "0 2 * * 0"  # Executa todo domingo às 02:00 AM
  enabled: true
```

---

# 5. Fluxo de Execução e Monitoramento

Ao executar o Job, o fluxo seguido pelo CDE é:

1.  **Provisionamento:** O CDE sobe um cluster Spark efêmero (Kubernetes pods).
2.  **Initialization:** O script carrega os JARs do mount para o Classpath.
3.  **Catalog Mapping:** O Driver consulta o Hive Metastore (via SDX/Ranger).
4.  **Distributed Scan:** Os Executors listam os objetos no S3 em paralelo.
5.  **Commit:** O Spark consolida os dados e faz o *append* na tabela Iceberg.
6.  **Deprovisioning:** O cluster é desligado, economizando créditos.

### Verificação de Logs:
No painel do Job, clique em **Logs** > **Driver Standard Output**. Graças ao `VERBOSE/INFO` configurado no script, você verá:
* `INFO - Table mapped: default.customers`
* `INFO - Aggregate metrics calculated`
* `INFO - Job completed successfully`

---

# 6. Consumo dos Dados

Após a execução, a tabela `sys_monitoring.lakehouse_health_history` estará disponível.

* **Impala:** Execute `INVALIDATE METADATA sys_monitoring.lakehouse_health_history` (necessário apenas se não for Iceberg).
* **Cloudera Data Visualization:** Crie um Dashboard filtrando pelo `audit_timestamp` mais recente para ver o estado atual ou use a série temporal para ver a evolução do desperdício de storage.

---

> **Dica de Administrador:** Se o seu ambiente tiver milhares de tabelas, monitore o tempo de execução no CDE. Se estiver demorando muito, aumente o número de `spark.executor.instances` para paralelizar ainda mais a listagem de arquivos no S3.