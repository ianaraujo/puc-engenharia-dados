# Projeto de Engenharia de Dados (PUC-Rio)

[![License: CC BY-NC](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc-sa/4.0/)

Trabalho de conclusão do módulo de Engenharia de Dados do curso de pós-graduação em Ciência de Dados e Analytics da PUC-Rio.

![Overview do Projeto](/images/overview-projeto.png)

> :bar_chart: O produto final deste trabalho é um dashboard, criado usando Metabase. Ele pode ser acessado publicamente [aqui](http://3.137.169.241:3000/public/dashboard/749258f2-67f9-41a4-9e57-f69c0fb5392b).

## Sumário

- [1. Objetivo](#objetivo)
  - [1.1 Plataforma](#plataforma)
  - [1.2 Perguntas](#perguntas)
- [2. Coleta](#coleta)
- [3. Modelagem](#modelagem)
  - [3.1 Delta Lakehouse](#delta-lakehouse)
  - [3.2 Linhagem dos Dados](#linhagem-dos-dados)
  - [3.3 Exemplo: Custo por Beneficiário](#exemplo-custo-por-beneficiário)
- [4. Carga](#carga)
  - [4.1 Export para AWS](#export-para-aws) 
  - [4.2 Databricks Workflows](#databricks-workflows)
- [5. Análise](#análise)
  - [5.1 Qualidade](#qualidade)
  - [5.2 Perguntas](#perguntas-1)
    - [5.2.1 Sinistralidade](#1-qual-é-o-atual-índice-de-sinistralidade-no-setor-de-seguros-de-saúde-ele-está-abaixo-ou-acima-da-média-histórica)
    - [5.2.2 Custo por Beneficiário e Market Share](#2-e-3-qual-é-a-seguradora-mais-eficiente-do-ponto-de-vista-de-custo-por-beneficiário-qual-é-o-market-share-em-número-de-beneficiários-no-segmento-médico-hospitalar)
    - [5.2.3 Número de Operadors](#4-quantas-empresas-de-plano-de-saúde-existem-no-brasil)
    - [5.2.4 Taxa de Cobertura](#5-quantos-beneficiários-existem-no-brasil-qual-é-a-taxa-de-cobertura)
    - [5.2.5 Planos Individuais e Coletivos](#6-existem-mais-planos-individuais-ou-coletivos)
  - [5.3 Metabase](#metabase)
- [6. Conclusão](#conclusão)
    - [6.1 Custos](#custos)

OBS: A estrutura de documentação deste projeto foi definida pelos critérios de avaliação do trabalho, portanto, refletem os tópicos de discussão exigidos.

## Objetivo

A proposta do trabalho consiste em construir uma pipeline de dados completa, que deve incluir ingestão/coleta, modelagem, transformação e carga, utilizando algum ambiente de computação em nuvem, como Databricks, AWS, GCP e Azure. Além disso, ao final, é necessário analisar os dados, a fim de responder perguntas previamente definidas.

O objetivo deste projeto é trabalhar com os dados da **Agência Nacional de Saúde Suplementar (ANS)** na plataforma do Databricks. A agência disponibiliza grandes volumes de dados sobre operadoras de planos de saúde e beneficiários, em formato aberto. Alguns conjuntos de dados ultrapassam 10 GB, o que representa um desafio interessante para colocar em prática os conhecimentos adquiridos sobre processamento distribuído, modelagem de daods em data lakehouses, e arquitetura Apache Spark.

### Plataforma

Em relação à plataforma utilizada, optei pelo uso do Databricks, na versão Premium. Além disso, optei pela AWS como ambiente para o workspace do Databricks, por já ter familiaridade com a plataforma.

A opção por não utilizar o Databricks Community (versão gratuita) foi feita visando ter mais contato com um ambiente "real", mais próximo do utilizado em empresas, e para ter acesso a algumas funcionalidades fundamentais, como a ferramenta de orquestração de pipelines (Databricks Workflows) e a conexão com repositórios no GitHub. Devido ao uso da versão Premium, a execução do trabalho incorreu em alguns custos, que serão apresentados e discutidos adiante.

Também foram utilizadas instâncias de servidor EC2, além dos clusters criados pelo Databricks, para seção de analytics do trabalho.

### Perguntas 

As perguntas/problemas que desejo responder através das análises são:

1. Qual é o atual **índice de sinistralidade** no setor de seguros de saúde? Ele está abaixo ou acima da média histórica?

2. Qual é a seguradora mais eficiente do ponto de vista de **custo por beneficiário**?

3. Qual é o **market share** em número de beneficiários no segmento médico-hospitalar?

4. Quantas empresas de plano de saúde existem no Brasil?

5. Quantos beneficiários existem no Brasil? Qual é a taxa de cobertura?

6. Existem mais planos individuais ou coletivos?

## Coleta

A ANS disponibiliza todos os seus dados abertos através de um servidor FTP, que pode ser acessado através do [link](https://dadosabertos.ans.gov.br/FTP/PDA/).

Os dados têm boa qualidade, no geral, e são bem organizados, sendo a grande maioria acompanhada de um arquivo de metadados ou catálogo. Alguns catálogos informam, inclusive, que algumas colunas são chaves estrangeiras de tabelas em outros conjuntos de dados, o que é muito útil.

Alguns dos dados utilizados, como apresentado, têm grandes volumes, como o cadastro de beneficiários ativos, divulgado mensalmente, que possui cerca de 10 GB em arquivos `.csv`, totalizando cerca de 14,5 milhões de registros e 22 atributos, para cada mês.

Os dados são disponibilizados em arquivos compactados `.zip`, por isso, o código para coleta de dados envolve a extração dos arquivos, leitura em memória, e persistência em um volume no Databricks (ou storage no S3), que serviu como landing/camada raw.

Esse procedimento de persistência dos dados em uma camada `raw`, fora do ambiente do Delta Lake, é uma forma de evitar problemas comuns ao se trabalhar com grandes volumes de dados e clusters Spark, como **spill**.

Após salvar os arquivos no storage, o código faz a leitura usando  `pyspark`, e a ingestão na camada bronze, em formato Delta.

O processo de ingestão dos dados e criação da camada bronze foi feito através de **classes de ingestão**, como:

bronze_beneficiarios.py

```python
class Collector:
    def __init__(self, spark: SparkSession, endpoint: str, year: str, month: str):

        self.year = year
        self.month = month
        self.spark = spark

        self.base_url = 'https://dadosabertos.ans.gov.br/FTP/PDA'
        self.url = f'{self.base_url}/{endpoint}/{year}{month}'

        self.volume = '/Volumes/raw/ans/beneficiarios'
        self.table_name = f'beneficiarios_{self.year}_{self.month}'

    def _fetch_data(self, url: str) -> requests.Response:
        try:
            response = requests.get(url)
            response.raise_for_status()

            return response
        
        except requests.HTTPError as e:
            print(f'HTTP Error: {e}')
            return None

    def _get_zip_urls(self) -> list[str]:
        soup = BeautifulSoup(self._fetch_data(self.url).content, 'html.parser')
        links = soup.find_all('a')

        zip_urls = [f"{self.url}/{url.get('href')}" for url in links if url.get('href').endswith('.zip')]

        return zip_urls
    
    def _extract_raw_files(self, zip_urls: list[str]) -> None:

        for url in zip_urls:
            content = BytesIO(self._fetch_data(url).content)

            with ZipFile(content, 'r') as zip_file:
                csv_files = [file for file in zip_file.namelist() if file.endswith('.csv')]
                
                if len(csv_files) != 1:
                    raise ValueError(f'Expected exactly one CSV file in {url}, found {len(csv_files)}')
                
                csv_file_name = csv_files[0]
                volume = f'{self.volume}/{csv_file_name}' 

                with zip_file.open(csv_file_name) as data:
                    csv_content = data.read()
                    
                with open(volume, 'wb') as f:
                    f.write(csv_content)
        
            print(f"Extracted '{csv_file_name}' to {volume}")

    def ingest_bronze(self, catalog: str, schema: str) -> None:
        zip_urls = self._get_zip_urls()
        self._extract_raw_files(zip_urls)

        df = spark.read.csv(self.volume, sep=';', header=True, inferSchema=True)
 
        df.write \
            .format('delta') \
            .mode('overwrite') \
            .saveAsTable(f'{catalog}.{schema}.{self.table_name}')

        print(f"Table '{self.table_name}' loaded successfully!")

    def run(self, catalog: str, schema: str) -> None:
        self.ingest_bronze(catalog, schema)

        print('Data ingestion completed!')
```

## Modelagem

Foi escolhido para o trabalho o modelo de **data lake**, que consiste em salvar os dados estruturados e não-estruturados, sem um schema definido, que serão trabalhados em outras etapas ou consumidos por aplicações.

O "problema" dos data lakes tradicionais é que os dados são salvos da mesma forma que foram capturados, o que significa - geralmente - que esses dados têm baixa qualidade e não passaram por *constraints* e camadas de processamento.

### Delta Lakehouse

Uma solução para esse problema, muito adotada por profissionais que utilizam a plataforma do Databricks pela fácil integração, é o framework open-source **Delta Lake** e a arquitura de data lakehouses.

Esse framework consiste em uma camada de abstração construída em cima do seu data lake tradicional (Amazon S3, por exemplo), que incentiva a criação de fluxos de dados em camadas, em que cada camada o dado é tratado e é trabalhado em níveis incrementais de qualidade.

Essas camadas são:

- **Bronze:** tabelas em formato bruto, com máximo de fidelidade ao dado original coletado;
- **Silver:** tabelas com estrutura e schema definidos, realizada limpeza e enriquecimento dos dados;
- **Gold:** tabelas agregadas com métrias de interesse de acordo com a regra de negócio.

<br>

![Delta Lake](/images/delta-lake.png)

### Linhagem dos Dados e Metastore

A pipeline do trabalho utiliza o framework Delta e as tabelas são processadas em camadas bronze, silver e gold. 

Ao salvar as tabelas em formato `delta` e utilizando o Unity Catalog, é possível usufruir de funcionalidades integradas do **metastore** do Databricks, como controle de acesso, métricas de uso, visualização de schema e linhagem dos dados.

Para ilustrar essa funcionalidade, podemos observar o diagrama de linhagem da tabela `gold.ans.custo_beneficiario`, que utiliza 4 (quatro) tabelas como origem.

![Linhagem do Custo por Beneficiário](/images/custo-beneficiario-lineage.png)

Podemos observar que no *schema* das tables da camada bronze (à esquerda da imagem) há maior dimensionalidade, além de dados com tipos inapropriados, nomes de colunas de diferentes formatos, entre outras características de dados em menor qualidade.

Um exemplo é na coluna "CNPJ" da tabela `bronze.ans.operadoras`, que ao ler os dados usando Spark, foi inferido um tipo `bigint`, enquanto o correto seria `string`. Essa transformação é feita na camada silver e na tabela `silver.ans.operadoras` já podemos ver a mudança feita. 

O código utilizado para transformação desses dados na camada silver pode ser encontrado em [/src/silver/silver_operadoras.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/silver/silver_operadoras.py).

silver_operadoras.py

```python
df = spark.sql(f'SELECT * FROM bronze.{schema}.{table}')

upper_cols = [col.upper() for col in df.columns]

df = df.toDF(*upper_cols)

final = df \
    .withColumn('REGISTRO_ANS', df['REGISTRO_ANS'].cast('string')) \
    .withColumn('RAZAO_SOCIAL', F.regexp_replace(F.col('RAZAO_SOCIAL'), r'[./]', '')) \
    .withColumn('RAZAO_SOCIAL', F.regexp_replace(F.col('RAZAO_SOCIAL'), r'\b(LTDA|SA|EIRELI|ME|EPP)\b', '')) \
    .withColumn('RAZAO_SOCIAL', F.regexp_replace(F.col('RAZAO_SOCIAL'), r' - $', '')) \
    .withColumn('CNPJ', df['CNPJ'].cast('string')) \
    .select(
        'DATA_REGISTRO_ANS',
        'REGISTRO_ANS',
        'CNPJ',
        'RAZAO_SOCIAL',
        'NOME_FANTASIA',
        'MODALIDADE'
    )

final = final.withColumn('NOME_FANTASIA', F.when(
    F.col('NOME_FANTASIA').isNull(), F.col('RAZAO_SOCIAL')).otherwise(F.col('NOME_FANTASIA')
))


final.write.mode('overwrite').format('delta').saveAsTable(f'silver.{schema}.{table}')
```

Destaque para o código que transforma os dados da coluna "CNPJ" em `string`.

```python
.withColumn('CNPJ', df['CNPJ'].cast('string'))
```

Além dessa transformação, na camada `silver`, é feito um tratamento da "RAZAO_SOCIAL", removendo termos comuns (LTDA, SA, EIRELI, etc). Quando "NOME_FANTASIA" é `NULL`, o código substitui o campo nulo pelo valor da "RAZAO_SOCIAL".

### Exemplo: Custo por Beneficiário

Dessa forma, conforme as tabelas avançam no fluxo, é definido um schema, até chegar na tabela *gold* que será consumida pelo ambiente de *analytics*, para geração de insights e produtos de dados, como dashboards.

A tabela `gold.ans.custo_beneficiario`, que calcula um indicador setorial relacionado à eficiência da operadora, é um bom exemplo, pois utiliza todas as 3 (três) fontes primárias para ser construída.

Na camada `gold`, na maioria dos casos, utilizei a linguagem SQL para criar as tabelas:

[custo_beneficiario.sql](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/gold/custo_beneficiario.sql)

```sql
CREATE OR REPLACE TABLE gold.ans.custo_beneficiario
USING DELTA AS (
  WITH despesas AS (
    SELECT ANO, REG_ANS, AVG(VL_SALDO_INICIAL) AS TOTAL_DESPESAS
    FROM silver.ans.demonstracoes_contabeis
    WHERE ANO = 2023 AND CD_CONTA_CONTABIL = '41'
    GROUP BY ANO, REG_ANS
  ),

  beneficiarios AS (
    SELECT CD_OPERADORA, SUM(TOTAL_BENEFICIARIOS) AS NUM_BENEFICIARIOS
    FROM gold.ans.num_beneficiarios
    GROUP BY CD_OPERADORA
  ),

  operadoras AS (
    SELECT * FROM silver.ans.operadoras
    WHERE LOWER(MODALIDADE) NOT LIKE '%odonto%'
  )

  SELECT d.REG_ANS, o.NOME_FANTASIA, ROUND(d.TOTAL_DESPESAS / b.NUM_BENEFICIARIOS, 2) AS CUSTO_BENEFICIARIO
  FROM despesas AS d
  LEFT JOIN beneficiarios AS b ON d.REG_ANS = b.CD_OPERADORA
  LEFT JOIN operadoras AS o ON d.REG_ANS = o.REGISTRO_ANS
  WHERE b.NUM_BENEFICIARIOS > 0 AND d.TOTAL_DESPESAS > 0
);
```

Ao final do fluxo de transformações, a tabela na camada `gold` possui apenas 3 (três) domínios, seguindo o catálogo abaixo:

| Variável | Tipo | Descrição |
| -------- | ------------ | --------- |
| REG_ANS | string | Código ANS de identificação da operadora |
| NOME_FANTASIA | string | Nome fantasia da operadora |
| CUSTO_BENEFICIARIO | double | Custo por beneficiário em reais (R$) por trimestre |

Essa tabela final está pronta para ser consumida por dashboards ou por *stakeholders* dentro da organização, sendo possível rankear as empresas da mais eficiente para a menos eficiente, assim como fazer *joins* com outras tabelas, como `gold.ans.market_share`, e comparar a eficiência entre as líderes do mercado.
 
## Carga

Todas as etapas da pipeline de ETL (extração, transformação e carga) foram feitas de forma automatizada e utilizando Python (pyspark) e SQL. 

Se considerarmos a etapa de ingestão na camada `raw`, o processo segue a lógica de ELT (extração, carga e transformção), de modo que os dados são coletados e carregados como arquivos no formato original (.csv), em uma *landing zone*, e só depois são transformados em tabelas `delta`.

A escolha por adotar esse processo foi devido ao grande volume dos conjuntos de dados de `demonstrações_contábeis` e `beneficiários`, além da eficiência gerada por essa abordagem, resultando em operações mais rápidas e sobrecarga reduzida nos clusters.

Todos os arquivos utilizados para construção da pipeline de ETL (ou ELT), separados em camadas bronze, silver e gold, podem ser consultados neste repositório na pasta `/src`.

Aqui estão os links para os arquivos:

#### Bronze

- [bronze_beneficiarios.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/bronze/bronze_beneficiarios.py)
- [bronze_demonstracoes_contabeis.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/bronze/bronze_demonstracoes_contabeis.py)
- [bronze_operadoras.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/bronze/bronze_operadoras.py)

#### Silver

- [silver_beneficiarios.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/silver/silver_beneficiarios.py)
- [silver_demonstracoes_contabeis.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/silver/silver_demonstracoes_contabeis.py)
- [silver_operadoras.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/silver/silver_operadoras.py)

#### Gold

- [sinistralidade.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/gold/sinistralidade.py)
- [custo_beneficiario.sql](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/gold/custo_beneficiario.sql)
- [num_beneficiarios.sql](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/gold/num_beneficiarios.sql)
- [num_operadoras.sql](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/gold/num_beneficiarios.sql)
- [market_share.sql](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/gold/market_share.sql)

### Export para AWS

Além dos códigos utilizados para pipeline dos dados da ANS, no diretório `/src` também pode ser encontrado o arquivo `export.py`.

Por ter usado o Databricks Premium, após o período de teste de 14 dias, não farei mais uso do workspace e ambiente de Delta Lake construído, em razão da incidência de custos adicionais.

Para manter os dados gerados, mesmo após encerrar o workspace no ambiente do Databricks, criei um *shared volume* - que liga o Databricks a um storage externo no ambiente da AWS, no meu caso o S3 - e um código em Python que carrega todas as tabelas da camada `gold` nesse volume em formato `parquet`.

Dessa forma, eu garanto o acesso ao dados do Databricks no meu ambiente da AWS, mesmo após encerrar meu período de utlização da plataforma.

O código utilizado para realizar essa etapa final foi:

export.py

```python
bucket = 'databricks-gold-ans'
database = 'gold.ans'

tables = spark.sql(f'show tables in {database}')


def export_parquet(bucket: str, database: str, table: DataFrame) -> None:
    df = spark.read.table(f'{database}.{table.tableName}') 

    save_path = f'/Volumes/gold/ans/{bucket}/{table.tableName}'
    
    df.repartition(1) \
        .write.mode('overwrite') \
        .format('parquet') \
        .option("header","true") \
        .option("inferSchema", "true") \
        .save(save_path)

    parquet_file = [file.name for file in dbutils.fs.ls(save_path) if file.name.startswith('part-')]

    dbutils.fs.mv(save_path + "/" + parquet_file[0], f"{save_path}.parquet")

    for file in dbutils.fs.ls(save_path):
        dbutils.fs.rm(file.path)
    
    dbutils.fs.rm(save_path)
    
    print(f"Saved '{table.tableName}.parquet' to {bucket}!")


for table in tables.collect():
    export_parquet(bucket, database, table)
    
    print("All data exported!")
```

### Databricks Workflows

Todos os processos descritos acima, incluindo a coleta dos dados, transformações, carga e exportação dos dados finais para AWS, foram automatizados utilizando a funcionalidade do Databricks Workflows. 

O Databricks Workflows é um serviço integrado na plataforma para fazer a **orquestração de pipelines**, de forma similar a outras soluções no mercado, como Airflow, Dagster, Prefect, etc.

Assim como os demais, é possível agendar execuções, *triggers*, monitorar falhas e logs, e definir de forma visual a ordem de execução das tarefas.

A grande vantagem do Databricks Workflows, frente aos outros serviços, é a integração com a plataforma. As tarefas de um workflow podem utilizar clusters dedicados - chamados de *job clusters*, que são ligado e depois terminados apenas para execução da pipeline - e podem ser definidas a partir dos próprios Notebooks do Databricks.

Dessa forma, é possível utilizar diversas liguagens, como Python, SQL, Scala e R, na mesma pipeline.

Ao final da configuração de um workflow, é também possível gerar um arquivo `.json`, que foi salvo em [workflows/pipeline.json](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/workflows/pipeline.json), e permite versionar os *jobs*, além de definir programaticamente uma visualização da sua pipeline:

![Databricks Workflows](/images/workflow-tasks.png)
![Databricks Workflows 2](/images/workflow-run.png)

Podemos observar que a pipeline teve duração total de 18 minutos e 27 segundos, e todas as tarefas foram concluídas com sucesso. No contexto de *big data* e janelas de produção *batch* não é um tempo muito grande, embora exista margem para tornar o código mais eficiente ou utilizar clusters mais potentes, o que aceleraria a execução da pipeline.

## Análise

A etapa de análise foi desenvolvida utilizando SQL em um ambiente de analytics criados fora no Databricks, na AWS, utilizando o Metabase, hospedado usando Docker. As tabelas da camada `gold` foram carregadas em um bancos de dados PostgreSQL, sendo possível realizar consultas, responder as perguntas definidas no início do trabalho e gerar visualizações.

### Qualidade

Se tratando de dados disponibilizados por uma agência reguladora, como é o caso da Agência Nacional de Saúde Suplementar (ANS), não tive grandes problemas em relação a qualidade dos dados.

Além do mais, a ANS possui um [Plano de Dados Abertos - PDA 2024-2026](https://www.gov.br/ans/pt-br/acesso-a-informacao/perfil-do-setor/dados-abertos-1#:~:text=Plano%20de%20Dados%20Abertos%20%2D%20PDA,Federal%20no%20%C3%A2mbito%20da%20ANS.), que visa implementar "ações de planejamento, promoção, execução e melhoria de ações estratégicas e operacionais relacionadas à Política de Dados Abertos", o que mostra uma preocupação com a qualidade da informação divulgada.

É possível encontrar todos os conjuntos de dados em um só lugar de maneira organizada e documentada. Os arquivos seguem padrões de nomenclatura, portanto, é fácil iterar por eles e encontrar de forma programática os dados que precisam ser coletados.

Conforme comentado no início do trabalho, a agência divulga catálogos e metadados sobre seus conjuntos, até mesmo informando sobre relacionamentos entre tabelas, que são bem normalizadas.

Portanto, não encontrei desafios frente a qualidade dos dados. As transformações que precisei fazer se concentraram em adequar o schema para o caso de uso e o tratamento de alguns poucos dados nulos.

### Perguntas

As perguntas poderiam ter sido respondidas no ambiente do Databricks, no entanto, escolhi criar um ambiente separado de analytics usando o [Metabase](https://www.metabase.com/), um serviço open-source de BI e dashboards.

No Metabase é possível definir "Questions", que podem ser consultas SQL. Após criar as *questions*, organizadas em coleções lógicas específicas, elas podem ser usadas para criar dashboards.

#### 1. Qual é o atual índice de sinistralidade no setor de seguros de saúde? Ele está abaixo ou acima da média histórica?

O índice de sinistralidade é um importante indicador de **rentabilidade** no setor de seguros, de forma abrangente. No segmento de planos médico-hospitalares não é diferente. Esse indicador consiste na razão entre os "eventos indenizáveis" (sinistros), ou seja, despesas com assistência médica, e o total de receitas obtidas através dos planos de saúde.

Uma métrica de sinistralidade elevada indica que uma grande parte da receita está sendo gasta com os custos referentes aos sinistros, o que pode sinalizar a necessidade de reajustes ou adequação dos serviços oferecidos para garantir a sustentabilidade financeira da operadora. 

Já um índice muito baixo pode indicar uma operação pouco competitiva ou a subutilização dos serviços de saúde, o que também deve ser cuidadosamente monitorado para garantir um equilíbrio justo entre a prestação de serviços e a viabilidade econômica da operadora.

```sql
SELECT ANO, AVG(SINISTRALIDADE) AS INDEX_SINISTRALIDADE
FROM sinistralidade
WHERE SINISTRALIDADE > 0 AND SINISTRALIDADE < 100
GROUP BY ANO
ORDER BY ANO ASC;
```

![Sinistralidade](/images/sinistralidade.png)

O atual índice de sinistralidade é de **71,79%**, que está um pouco acima da média histórica, considerando os últimos anos.

O que chama atenção no gráfico é o ano de 2020, em que se observa uma queda fora do padrão da sinistralidade. Esse fenômeno ocorreu em razão da pandemia do COVID-19, que iniciou em 2020, e fez com que a procura por atendimentos médicos tivesse uma forte queda.

Essa constatação pode parecer contraintuitiva, mas o que observou-se foi uma redução da procura por consultas e procedimentos não emergenciais, exames de rotinas, entre outros, o que impactou na sinistralidade. Após o período do isolamento, houve um crescimento acentuado da sinistralidade, devido a demanda reprimida do período da pandemia, o que deixou muitas empresas do segmento em situação financeira pouco confortável.

#### 2 e 3. Qual é a seguradora mais eficiente do ponto de vista de custo por beneficiário? Qual é o market share em número de beneficiários no segmento médico-hospitalar?

O custo por beneficiário é uma métria de **eficiência** das operadoras de planos de saúdes, que calcula o valor médio gasto pela operadora para fornecer os serviços de saúde para cada beneficiário durante um ano. 

As operadoras atuam em diferentes modelos de negócio, que resultam em estruturas de custo diferentes. Modelos de negócio mais verticalizados, ou seja, operadoras que possuem seus próprios hospitais, clínicas e laboratórios, tendem a ter um controle maior sobre a operação, consequentemente sobre os custos, a qualidade dos serviços prestados e eliminando intermediários. 

Outro fator que pode impactar no custo por beneficiário é o market share, que indica o **tamanho** da empresa no mercado em comparação aos concorrentes. O market share poder ser medido de diferentes formas, mas nesse caso está sendo considerado o número de beneficiários.

Empresas maiores, especialmente nesse setor, costumam apresentar ganhos de eficiência e vantagens competitivas, em razão do tamanho, pois a operadora pode otimizar os recursos, negociando melhores preços de medicamentos e equipamentos, em função da escala. Isso também tem grande impacto no custo por beneficiário.

```sql
SELECT 
    CASE 
        WHEN LENGTH(market_share.NOME_FANTASIA) > 16 THEN SUBSTRING(market_share.NOME_FANTASIA FROM 1 FOR 16) || '...'
        ELSE market_share.NOME_FANTASIA
    END AS NOME_FANTASIA,
    market_share.MARKET_SHARE,
    custo_beneficiario.CUSTO_BENEFICIARIO
FROM market_share
LEFT JOIN custo_beneficiario
ON market_share.CD_OPERADORA = custo_beneficiario.REG_ANS
LIMIT 10;
```

![Market Share x Custo por Beneficiário](/images/share_custo.png)

A Hapvida, que é líder do setor, recentemente adquiriu a Notre Dame Intermédica e todas sua rede hospitalar, consolidando um conglomerado de quase **15%** do mercado de planos de saúde, cerca de **7,65 milhões** de beneficiários.

A Hapvida x Intermédica é uma empresa verticalizada, ou seja, ela comercializa os planos de saúde e é dona dos hospitais que prestam serviço para esses planos, o que permite a empresa ter um controle muito maior sobre suas margens, além de se aproveitar dos ganhos de escala.

Esses fatores combinados são os motivos da empresa ter um dos menores custos por beneficiário do mercado, tendo o menor custo entre as 10 maiores, de apenas **R$ 717,54** por beneficiário/ano.

Na ponta oposta, como exemplo, podemos citar o Bradesco Seguros, que não possui nenhum ou rede própria, e terceiriza a prestação dos serviços oferecidos pelos seus planos. O Bradesco, que atualmente é a segunda maior empresa do segmento, se consideramento a junção entre Hapvida e Intermédica, possui um custo médio por beneficiário de **R$ 7.200,00** por ano.

#### 4. Quantas empresas de plano de saúde existem no Brasil?

Atualmente, existem **850** operadoras ativas no Brasil.

```sql
SELECT DISTINCT COUNT(*) FROM num_operadoras;
```

#### 5. Quantos beneficiários existem no Brasil? Qual é a taxa de cobertura?

Existem no Brasil cerca de **51 milhões** de beneficiários de planos médico-hospitalares, com ou sem assistência odontológica. Se considermos o tamanho da população brasileira de cerca de 215 milhões, calculamos uma taxa de cobertura de aproximadamente **23%**.

Isso significa que menos de 1/4 da população brasileira possui plano de saúde.

```sql
SELECT DISTINCT SUM(TOTAL_BENEFICIARIOS) FROM num_beneficiarios;

SELECT DISTINCT SUM("TOTAL_BENEFICIARIOS") / 215300000 FROM num_beneficiarios;
```

#### 6. Existem mais planos individuais ou coletivos?

Entre os mais de 50 milhões de beneficiários, somente **17%** deles possuem planos individuais ou familiares. A grande maioria dos planos são coletivos, somando **83%**, sendo em grande parte planos empresariais.

```sql
WITH total_beneficiarios AS (
    SELECT SUM("TOTAL_BENEFICIARIOS") AS "TOTAL" FROM num_beneficiarios
)

SELECT 
    "TIPO_CONTRATACAO_PLANO", 
    ROUND(SUM("TOTAL_BENEFICIARIOS") / total_beneficiarios."TOTAL", 2) AS "BENEFICIARIOS_RATIO"
FROM num_beneficiarios, total_beneficiarios
GROUP BY "TIPO_CONTRATACAO_PLANO", total_beneficiarios."TOTAL";
```

![Planos](/images/pizza.png)

### Metabase

Além do ambiente do Databricks, também utilizei um ambiente de analytics criado usando Docker, PostgreSQL e Metabase, em uma instância EC2 na AWS.

Além das tecnologias mencionadas, também foi escrito um código em Python ([app/main.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/app/main.py)) responsável por fazer a carga *full-load* dos arquivos `parquet` salvos no S3 diretamente nas tabelas do banco de dados PostgreSQL.

Mais informações podem ser encontradar no arquivo [app/README.md](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/app/README.md) dedicado exclusivamente para explicar a configuração desse ambiente.

Além de utilizar o Metabase como ambiente para realizar as consultas e produzir as visualizações, também criei um **dashboard** público, que pode ser acessado livremente através do link:

> http://3.137.169.241:3000/public/dashboard/749258f2-67f9-41a4-9e57-f69c0fb5392b

![Dashboard](/images/dashboard.png)

## Conclusão

Em forma de avaliação final do trabalho, acredito ter conseguido atingir todos os objetivos propostos. Sobretudo, considero a escolha do Databricks, como plataforma, muito acertada, frente as demais opções. Apesar dos custos adicionais, o Databricks oferece muitas ferramentas para profissionais que trabalham com dados, engenheiros, cientistas e analistas de dados, que justificam os custos em razão do ganho de produtividade. 

Outro ponto fundamental foi aprendizado e o contato com as funcionalidades da plataforma, que cada vez mais é adotada por empresas no mercado, mas também por permitir trabalhar em soluções mais avançadas e mais próximas do dia-a-dia das organizações.

### Custos

Desde o início do trabalho, planejei utilizar os 14 dias do free-trial do Databricks Premium integralmente e configurei um *budget* de cerca de R$ 150,00 para realização do trabalho, conforme os custos da AWS. Esse objetivo também foi atingido.

No Databricks, utilizei um cluster de 4 cores e 16 GB de memória, durante todo o período, que se provou suficiente para as tarefas executadas. Principalmente, considerando um bom equilíbrio entre custo e performance.

No total, desde o dia 27 de junho, tive custo total com o trabalho de $20.63, representado por dois serviços principais na AWS. As barras em azul representam um custo fixo essencial para o funcionamento do workspace do Databricks, que é o NAT Gateway, que atua na configuração de rede dos clusters. Já as barras na cor vermelha são, de fato, os cluster criados para execução dos *workloads* do trabalho.

![Custos](/images/aws-costs.png)
