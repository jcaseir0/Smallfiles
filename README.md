# Contexto dos arquivos pequenos no Hadoop

## Funcionamento do HDFS NameNode

O HDFS foi desenvolvido para tratar eficientemente arquivos grandes, baseado no tamanho do bloco. A plataforma da Cloudera utiliza como padrão o tamanho do bloco com 128MB. Ou seja, é recomendável que os dados ingeridos no cluster sejam tratados e somente ser armazenados com o tamanho igual ou maior do que o tamanho do bloco. No caso de grandes volumes, avaliar também os tamanhos quando utilizar algoritmos de compactação. 

Avaliando o NameNode, serviço de categoria Master, no quesito de uso de memória: todo arquivo, diretório e bloco representam um objeto na memória com a ocupação de 150 bytes, como regra geral. Com base nessa informação, é possível dimensionar quanto deverá ser utilizado de memória na escolha do Hardware.

**Exemplo:** Para 10 milhões de arquivos, utilizando um bloco cada, necessitaríamos em média de 3Gb de memória para o processo do Namenode.

Uma informação importante é que o limite total de arquivos conhecidos para o HDFS, para garantir alto desempenho é de 300 milhões de arquivos.

## Impactos no HDFS NameNode (NN)

Avaliando as funções do serviço, quando temos uma grande quantidade de arquivos pequenos no HDFS, é gerado um alto consumo de buscas de localicação dos blocos onde se encontram os dados pequisado e perda de tempo nessa consulta, pela alta distribuição da infromação, além da solicitação dos arquivos, quando menores do que deveria, aumentando na quantidade de requisições entre os DataNodes. 
Ilustrando o cenário dos problemas de arquivos pequenos:

**Cenário de um arquivo grande de 192MiB**
- 1 arquivo = 1 bloco de 128MB + um bloco de 64MB, depois da replicação teríamos: 3 blocos de 128MB e 3 blocos de 64MB, fazendo a conta de utilização de memória:
  - 150 bytes * (1 arquivo inode + (Nro. de blocos * fator de replicação)) = 150 * (1 + (2 * 3)) = 1050 Bytes \\approx 1KB

**Cenário com 192 arquivos pequenos de 1MiB cada**
- 192 arquivos = 192 blocos de 128MB , depois da replicação teríamos: 576 blocos de 128MB, fazendo a conta de utilização de memória:
  - 150 bytes * (1 arquivo inode + (Nro. de blocos * fator de replicação)) = 150 * (192 + (192 * 3)) = 115200 Bytes \\approx 112KB

**Conclusão:** Com o uso de arquivos pequenos seria necessário mais de 100 vezes o uso de memória para tratar o mesmo volume.

## Apresentando os problemas com mais detalhes

- O NN mantém os registros de alteração de localização dos blocos no cluster, com muitos arquivos pequenos, o NN pode apresentar erro de falta de memória antes da falta de espaço em disco dos DataNodes (DN);
- Os DNs fazem o relatório de alteração de blocos para o NN através da rede, quanto mais blocos, mais alterações para ser reportados através da rede, com isso aumentando o fluxo e utilização de banda na comunicação;
- Quanto mais arquivos, mais solicitações de leitura precisam ser respondidas pelo NN, com isso aumentando a fila de RPC e a latência do processamento, degradando o desempenho e tempo de resposta. Uma demanda de RPC aceitável seria perto de 40K~50K RPCs/s;
- Pensando na manutenção do serviço, quando o NN reinicia, todos os metadados são carregados do disco para a memória, com arquivos pequenos, o tamanho dos metadados aumenta e torna o reinício lento.

Em face aos possíveis impactos, principalmente de lentidão do cluster, faz-se necessário o uso de ferramentas para identificação dos ofensores e visualização dos objetos que estão gerando os problemas, para facilitar a criação de um plano de ação.

--- 

## Como funciona no Public Cloud - Armazenamento no Amazon S3

No Cloudera Public Cloud (AWS), o problema de "Small Files" (arquivos pequenos) é amplificado pelo fato de o armazenamento primário ser o Amazon S3, e não o HDFS local. Diferente do HDFS, o S3 é um sistema de armazenamento de objetos onde cada operação de metadados tem um custo financeiro e de latência significativo.

### O Problema da Latência de Metadados (S3 vs. HDFS)

No HDFS, listar arquivos é uma operação de memória no NameNode. No S3, cada arquivo pequeno exige uma chamada de API (`GET` ou `LIST`).

* **Impala:** O Impala é extremamente sensível a isso. Ele precisa buscar os metadados e os file handles de cada arquivo. Se uma tabela tem 10.000 arquivos de 10 KB em vez de um arquivo de 100 MB, o Impala gastará mais tempo "conversando" com a API do S3 do que processando dados. Isso gera o sintoma de alta latência de planejamento no Cloudera Observability.

* **Hive:** O Hive (Tez) consegue agrupar (combinar) pequenos arquivos durante a execução do job usando o `CombineHiveInputFormat`, mas isso consome tempo extra de CPU e recursos de container antes do processamento real começar.

### Impacto nos Recursos Computacionais e Custos

No ambiente de produção da AWS, arquivos pequenos drenam dinheiro de três formas:

#### A. Custos de API do S3

A AWS cobra por mil requisições `GET/LIST`. Milhões de arquivos pequenos em jobs recorrentes podem gerar uma linha de custo surpreendente na fatura da AWS, antes mesmo de considerarmos o processamento.

#### B. Eficiência de CPU e I/O

* **I/O Overhead:** O throughput (MB/s) do S3 é otimizado para leituras sequenciais de arquivos grandes. Arquivos pequenos impedem o read-ahead eficiente.

* **Memory Pressure:** Cada arquivo processado exige que o Impala ou Hive mantenha metadados em memória. Milhões de arquivos podem levar ao esgotamento da memória heap do **Catalog Server** e do **Statestore** no Cloudera Manager.

* **Desperdício de "Virtual Clusters" (CDE/CDW):** Se você usa Cloudera Data Warehouse (CDW) na AWS, o Auto-scaling é baseado na carga. Arquivos pequenos fazem com que os executores fiquem "presos" em I/O de rede por mais tempo. Isso impede que o cluster desligue ou diminua (downscale), mantendo máquinas EC2 ligadas apenas para esperar respostas da API do S3.

### Estratégias de Mitigação (O que fazer no CDP)

Como Administrador, você deve implementar políticas para consolidar esses dados:

1. **Compaction Automática:** No Hive, utilize tabelas Iceberg (nativas no CDP). O Iceberg permite gerenciar o layout dos arquivos e executar comandos de OPTIMIZE ou REWRITE DATA FILES para consolidar arquivos pequenos sem downtime.
2. **S3 Guard / Metadata Caching:** Certifique-se de que o caching de metadados do Impala está bem configurado para evitar chamadas repetitivas ao S3.
3. **Ajuste no Spark (CDE):** Se os arquivos pequenos vêm de jobs de Data Engineering, ajuste o spark.sql.shuffle.partitions ou use o Coalesce/Repartition antes da escrita final no S3.
4. **HDFS como "Landing Zone":** Para volumes muito altos de arquivos minúsculos (ex: logs de IoT), use um cluster IaaS com HDFS como área de pouso, consolide os dados e só então mova para o S3 (Cold Storage) em arquivos maiores (idealmente 128MB a 512MB).

### Diagnóstico via Observability

No Cloudera Observability, procure por queries com alto **"Filesystem Read Wait Time"**. Se o tempo de espera de leitura for desproporcional ao volume de bytes lidos, você confirmou o problema de arquivos pequenos.

O formato **Apache Iceberg** é atualmente a melhor solução no CDP para resolver esse problema de forma transparente para o usuário.

--- 

## Tutoriais

- [On-premises](tutorials/on-prem-steps.md)
- [Public cloud](tutorials/public-cloud-steps.md)
- [Cloudera Data Visualization](tutorials/dataviz_commandcenter.md) (Em construção)