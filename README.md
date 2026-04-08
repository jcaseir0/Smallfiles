# Contexto dos arquivos pequenos no HDFS
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

## Tutoriais

- [On-premises](on-prem-steps.md)
- [Public cloud](public-cloud-steps.md)