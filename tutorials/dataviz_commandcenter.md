# Centro de Comando no Cloudera DataViz (Em construção))

Para habilitar o **Cloudera Data Visualization (CDV)** e transformar os dados coletados pelo seu Job de CDE em inteligência de negócio, siga os passos abaixo. O objetivo é criar um "Centro de Comando" que identifique os maiores ofensores de armazenamento e facilite a conversa técnica com as áreas usuárias.

---

## 1. Habilitando o Cloudera Data Visualization no CDW

O CDV é um serviço que roda "acoplado" a um **Virtual Warehouse** (geralmente Impala).

1.  **Acesse o Cloudera Data Warehouse:** No console do CDP, clique no serviço **Data Warehouse**.
2.  Para **Habilitar o serviço:**, no menu lateral esquerdo, clique em **Data Visualization**.
    * Se não houver uma instância criada, clique em **ADD NEW**.
    * Vincule-a ao seu **Virtual Warehouse Impala**:
      - **Name:** Defina um nome para a instância do DataViz
      - **Environments:** Selecione o ambiente onde se encontra o seu metadados
      - **User Groups:** Essa opção permite restringir um grupo de usuários para acesso.
      - **Admin Groups:** Essa opção permite restringir um grupo de usuários de administração.
      - **Tagging:** 
        - **Enter key:** departamento
        - **Enter value:** Big Data Support
      - **Resource Template:** `Default resources` ou algum opção que atenda a quantidade de usuários que irá acessar o relatório.
      - **Create**
3.  **Acesso:** Após a criação da instância, irá aparecer um sinal de status em frente do nome, assim que ficar verde, clique no ícone de **Data VIZ**.

---

## 2. Configuração do Dataset

Antes de criar gráficos, você precisa definir a fonte de dados.

1.  No CDV, vá em **Data** > **NEW CONNECTION**.
2.  Clique em **Connection Settings** e preencha conforme abaixo:
    1.  **Connection type:** Cloudera Data Warehouse Impala
    2.  **Connection name:** metadata_audit
    3.  **Cloudera Data Warehouse Virtual Warehouse:** Selecione o seu virtual Warehouse
    4.  AS informações de conexão serão preenchidas de forma automática:
        *  Hostname or IP Address
        *  Port #
    5.  Clique em TEST e verifique se a notificação em verde aparece: **Connection verified**.
    6.  Clique em **CONNECT**
3. Depois será necessário criar o **DataSet**, clique em **NEW DATASET**
   1.  Defina o nome em **Dataset Title:** `lakehouse_audit_history`
   2.  Escolha a fonte dos dados em **Dataset Source:** `From Table`.
   3.  Selecione o banco de dados em **Select Database:** `sys_monitoring`
   4.  Escolha a tabela em **Select Table:** `lakehouse_health_history`.
   5.  Clique em **CREATE**
4. **Dica Técnica:**
   1.  Clique no novo dataset criado e no menu lateral esquerdo **Fields**
   2.  Verifique se as configurações abaixo já estão aplicadas, se não, clique em **EDIT FIELDS** e corrija:
   3.  Marque a coluna `audit_timestamp` como tipo *Date/Time* e as métricas (`total_size_bytes`, `small_files_count`) como *Measure*.

---

## 3. Criação do Dashboard: "Lakehouse Health Command Center"

Crie um novo Dashboard e adicione os seguintes visuais para engajar as áreas de negócio. Para isso, volte para os detalhes do dataset no menu lateral esquerdo **Dataset Detail** e clique em **NEW DASHBOARD**:

1. Defina o nome do dashboard: `Centro de Comando - Governança de dados`
2. Defina um subtítulo: `Identificação dos maiores ofensores de armazenamento`

Crie os seguintes visuais:

### A. Kpis de Impacto
* **Título:** Quantidade de tabelas ofensoras
* **Subtítulo:** Mostrar o tamanho do problema logo no topo.
* **Big Number 1:** Soma de `total_size_bytes` (convertido para TB no cálculo).
* **Big Number 2:** Média de `small_files_pct` global.

### B. Distribuição por Área de Negócio (TreeMap)
* **Dimensão:** `owner` ou prefixo do `db_name`.
* **Métrica:** `total_size_bytes`.
* **Visual:** Um TreeMap onde o tamanho do quadrado representa o volume de dados. Isso identifica visualmente qual departamento "manda" no storage.

### C. Quadrante de Eficiência (Scatter Chart)
* **Eixo X:** `total_files_count`.
* **Eixo Y:** `small_files_count`.
* **Dimensão:** `table_name`.
* **Propósito:** Tabelas no canto superior direito são as **ofensoras críticas**. É com esses nomes que você deve ir até os usuários.


### D. Lista Técnica para Manutenção (Table View)
* **Colunas:** `db_name`, `table_name`, `partitioning_type`, `small_files_count`, `small_files_pct`.
* **Filtro:** `small_files_pct > 70%`.
* **Ação de Negócio:** Use esta lista como "Pauta de Reunião" para sugerir a aplicação de `OPTIMIZE` ou revisão da lógica de partição.

---

## 4. Iniciando os Trabalhos de Manutenção com os Usuários

Com o dashboard pronto, a abordagem com as áreas usuárias deve ser baseada em **Custo e Performance**:

1.  **Justificativa de Performance:** Explique que tabelas com 90% de arquivos pequenos tornam as consultas no Impala/Tableau/PowerBI lentas.
2.  **Proposta de Correção:**
    * **Para Tabelas Iceberg:** Informe que você agendará um processo de `rewrite_data_files`.
    * **Para Tabelas Hive:** Sugira a recreação da tabela com um particionamento mais "grosso" (ex: trocar partição por hora por partição por dia).
3.  **Gamificação:** Utilize o dashboard para mostrar o "Antes" e o "Depois" da manutenção, criando um ranking de áreas que mais otimizaram seus dados.



---

## 5. Passo Final: Automatização do Filtro de Auditoria

Para que o Dashboard mostre sempre a última foto:
* No Dataset do CDV, adicione um **Mandatory Filter** (Filtro Obrigatório):
    ```sql
    audit_timestamp = (SELECT MAX(audit_timestamp) FROM sys_monitoring.lakehouse_health_history)
    ```
Isso garante que, ao abrir o Dashboard, o usuário veja a situação atual e não uma média histórica poluída.

Este ecossistema (CDE para coleta -> CDW para armazenamento -> CDV para visualização) fecha o ciclo de governança proativa do seu Lakehouse.