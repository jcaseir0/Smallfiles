# Guia de Auditoria de Saúde do Lakehouse (Versão 2.4)

Este documento detalha o funcionamento do pipeline de monitoramento de metadados e arquivos pequenos utilizando **Cloudera Data Engineering (CDE)**.

## 1. O Script PySpark (`lakehouse_audit_pt.py`)
O script foi projetado para ser resiliente e escalável, utilizando as seguintes camadas de lógica:
* **Cabeçalho de Licença:** Garante os direitos autorais e permite modificações com menção ao autor original.
* **Parametrização Dinâmica:** Utiliza `sys.argv` para definir o tamanho do arquivo pequeno (threshold), com padrão de **5MB** caso não seja informado.
* **Mapeamento de Metadados:** Coleta UUID, Owner, Tipo de Particionamento e Estatísticas Lógicas via `DESCRIBE EXTENDED`.
* **Scan Distribuído:** Utiliza `mapPartitions` e bypass de Gateway para listar arquivos físicos no Storage de forma paralela sem sobrecarregar o Driver.
* **Governança Iceberg:** Função dedicada de manutenção para evitar que a própria tabela de auditoria sofra com arquivos pequenos através de `rewrite_data_files` e `expire_snapshots`.

---

## 2. Preparação e Integração com GitHub
Uma das melhores práticas no **CDE** é utilizar **Resources de Repositório** para versionamento.

1.  **Criação do Resource a partir do Git:**
    * No console do CDE, vá em **Repositories** > Selecione o seu virtual cluster > **Create Repository**.
    * **Nome do repositório:** `SmallfilesRepo`.
    * **URL:** `https://github.com/jcaseir0/Smallfiles`.
    * **Branch:** main
    * **Create**

Após a criação, aparecerá uma linha com o novo repositório criado e o **Status** deve aparecer **Active**.

---

## 3. Criação do Job no CDE (Interface Web)

1.  No console do **CDE**, vá em **Jobs** > Garanta que esteja no seu virtual cluster > **Create Job**.
2.  **Job Type:** `Spark 3.4.5`.
3.  **Name:** `metadata_audit_01`.
4.  **Select Application File:** Selecione **Repository**
5.  Clique no link **Add from Repository** e selecione o arquivo de dentro do diretório `smallfiles` > `cde` > Selecione o arquivo `lakehouse_audit_pt.py` e clique em **Select File**.
6.  **Arguments:** Informe o tamanho do arquivo pequeno (ex: `10` em Megabytes). Se vazio, o padrão será `5MB`.
7.  **Spark Configurations:**
    * spark.sql.iceberg.handle-timestamp-without-timezone=true
    * spark.memory.offHeap.enabled=true
    * spark.memory.offHeap.size=2g
    * spark.executor.memoryOverhead=2g
8. Na sessão **Advanced Configuration**:
   1. Executors: 5-10
   2. Initial Executors: 5
   3. Driver Cores: 2
   4. Executor Cores: 2
   5. Driver Memory: 4
   6. Executor Memory: 8
9.  Na sessão **Schedule**, utilizaremos a seguinte configuração:
    * **Basic**
    * Every `year`on `every day`of `every month` at `22`:`0`
    * Start Date: `selecione o dia que irá iniciar a coleta`
    * End Date: `selecione o dia que irá parar a coleta`
10. Clique em **Schedule**. 

---

## 4. Exemplos de Consultas no Impala
Com os dados persistidos na tabela `sys_monitoring.lakehouse_health_history`, você pode realizar análises diretamente no Impala para alimentar o **Cloudera Data Visualization**.

### A. Quantidade de Tabelas por Database

```sql
SELECT db_name, COUNT(DISTINCT table_name) as qtd_tabelas
FROM sys_monitoring.lakehouse_health_history
WHERE audit_timestamp = (SELECT MAX(audit_timestamp) FROM sys_monitoring.lakehouse_health_history)
GROUP BY db_name
ORDER BY qtd_tabelas DESC;
```

### B. Top 10 Tabelas com Maior Quantidade de Arquivos Pequenos

```sql
SELECT db_name, table_name, small_files_count, total_files_count, small_files_pct
FROM sys_monitoring.lakehouse_health_history
WHERE audit_timestamp = (SELECT MAX(audit_timestamp) FROM sys_monitoring.lakehouse_health_history)
ORDER BY small_files_count DESC
LIMIT 10;
```

### C. Proporção de Tabelas Particionadas vs. Não Particionadas

```sql
SELECT partitioning_type, COUNT(*) as total_tabelas
FROM sys_monitoring.lakehouse_health_history
WHERE audit_timestamp = (SELECT MAX(audit_timestamp) FROM sys_monitoring.lakehouse_health_history)
GROUP BY partitioning_type;
```

### D. Consulta de Integridade de uma tabela específica pelo UUID

```sql
SELECT * FROM sys_monitoring.lakehouse_health_history 
WHERE uuid = '7e9b5f3a1c2d4e5f6a7b8c9d0e1f2a3b' -- Exemplo de Hash MD5
ORDER BY audit_timestamp DESC 
LIMIT 1;
```
---

## 5. Fluxo de Funcionamento e Manutenção
Ao ser executado, o CDE provisiona o ambiente e carrega o código diretamente do seu repositório Git. Após a inserção dos dados (Append), a função de manutenção garante que a tabela `lakehouse_health_history` seja otimizada.

* **Rewrite:** Os arquivos de auditoria de execuções passadas são fundidos em arquivos maiores.
* **Expire:** Snapshots com mais de 30 dias são removidos para manter o storage limpo.

> **Dica:** Utilize o parâmetro de agendamento (Cron) para executar este Job fora do horário de pico, garantindo que o dashboard no Data Visualization esteja sempre atualizado para a reunião de Daily ou Revisões de Governança de dados.

