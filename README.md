# Engenharia de Dados - PUC-Rio

[![License: CC BY-NC](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc-sa/4.0/)

Trabalho de conclusão do módulo de Engenharia de Dados do curso de Pós-graduação em Ciência de Dados e Analytics da PUC-Rio.

![Overview do Projeto](/images/overview-projeto.png)

## Sumário

- [1. Objetivo](#objetivo)
  - [1.1 Plataforma](#plataforma)
  - [1.2 Perguntas](#perguntas)
- [2. Coleta](#coleta)
- [3. Modelagem](#modelagem)
  - [3.1 Delta Lakehouse](#delta-lakehouse)
  - [3.2 Linhagem dos Dados](#linhagem-dos-dados)
- [4. Carga](#carga)

## Objetivo

A proposta do trabalho consiste em construir uma pipeline de dados completa, que deve incluir ingestão/coleta, modelagem, transformação e carga, utilizando algum ambiente de computação em nuvem, como Databricks, AWS, GCP e Azure. Além disso, ao final, é necessário analisar os dados, a fim de responder perguntas previamente definidas no início do trabalho. 

Meu objetivo é trabalhar com os dados da **Agência Nacional de Saúde Suplementar (ANS)** na plataforma do Databricks. A agência disponibiliza grandes volumes de dados sobre operadoras de planos de saúde e beneficiários, em formato aberto. Alguns conjuntos de dados ultrapassam 10GB, o que representa um desafio interessante para colocar em prática os conhecimentos adquiridos sobre processamento distribuído, modelagem de data lakehouses, e arquitetura Apache Spark. 

### Plataforma

Em relação a plaforma utilizada, optei pelo uso do Databricks na versão Premium, utilizando os 14 dias de free-trial, que se encerram hoje (10/07/2024). Além do Databricks, escolhi a AWS como workspace do Databricks, por já ter alguma familiaridade com a plataforma.

A opção por não utilizar o Databricks Community foi visando ter mais contato com uma ambiente "real", mais próximo do utilizado em empresas, e para ter acesso a algumas funcionalidade fundamentais, como a ferramenta de orquestração de pipelines (Databricks Workflows), conexão com reposítório no GitHub, entre outras.

Também utilizei algumas instâncias EC2, além dos clusters criados pelo Databricks, para parte de analytics do trabalho, que vou apresentar mais para frente.

### Perguntas 

As perguntas/problemas que desejo responder através das análises são:

- Qual é o atual índice de sinistralidade no setor de seguros de saúde? Ele está abaixo ou acima da média histórica?

- Qual é a seguradora mais eficiente do ponto de vista de custo por beneficiário?

- Quantas empresas de plano de saúde existem no Brasil?

- Existem mais planos individuais ou coletivos?

- Quantos beneficiários existem no Brasil? Qual é a taxa de cobertura?

- Qual é o market share em número de beneficiários no segmento médico-hospitalar?

## Coleta

A ANS disponbiliza todos os seus dados abertos através de um servidor FTP em sua Plataforma de Dados Abertos, que pode ser acessado através do [link](https://dadosabertos.ans.gov.br/FTP/PDA/).

Os dados tem boa qualidade, no geral, e são bem organizados, sendo a grande maioria acompanhada de uma arquivo de metadados ou catálogo. Algums catálogos informam, inclusive, que alguns campos são chaves estrangeiras de tabelas em outros conjuntos de dados, o que é muito útil.

Alguns dados utlizados, como apresentado, tem grandes volumes, como o cadastro de beneficiário ativos, divulgado mensalmennte, que possui cerca de 10GB em arquivos .csv, totalizando cerca de 14,5 milhões de registros e 22 atributos.

Os dados são disponibilizados em arquivos compactados .zip, por isso, o código para coleta de dados envolvia, quase sempre, extrair os arquivos, lê-los em memória, e salvar em um volume no Databricks, que serviu como landing/camada raw.

Após salvar os arquivos no storage, o script fazia a leitura usando `pyspark`, e fazia ingestão na camada bronze, em format Delta. 

O processo de ingestão dos dados e criaçã da camada bronze foi feito através de **classes de ingestão**, como:

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

O modelo de dados escolhido para o trabalho foi o Data Lake, que consiste salvar em um storage dados estruturados e não-estruturados, sem um schema definido, que serão trabalhados em outras etapas ou consumidos por aplicações.

O problema dos data lakes tradicionais é que os dados são salvos da mesma forma que foram capturados, o que significa - geralmente - que esses dados tem baixa qualidades e não passaram por *constrains* e camadas de processamentos.

### Delta Lakehouse

Uma solução para esse problema, muito utilizada na plataforma do Databricks, é o framework open-source **Delta Lake** e a arquitura de Data Lakehouses.

Esse framework consiste em uma camada de abstração construída em cima do seu data lake tradicional (Amazon S3, por exemplo), que incentiva a criação de fluxos de dados em camadas, em que cada camada o dado é tratado e é incrementado em nívle de qualidade.

Essas camadas são:

- **Bronze:** tabelas em formato bruto, com máximo de fidelidade ao dado coletado;
- **Silver:** tabelas com estrutura e schema definidos, realizada limpeza e enriquecimento nos dados;
- **Gold:** tabelas agregadas com métrias de interesse de acordo com a regra de negócio.

![Delta Lake](/images/delta-lake.png)

### Linhagem dos Dados

A pipeline construída no trabalho utiliza o framework Delta e as tabelas são processadas em camadas bronze, silver e gold. Para ilustrar esse processo de modelagem, podemos observar a linhagem da tabela "gold.ans.custo_beneficiario", que utiliza 3 tabelas bronze como fonte.

![Linhagem do Custo por Beneficiário](/images/custo-beneficiario-lineage.png)

Podemos observar como as tabelas na camada bronze (à esquerda da imagem) possuem mais dimensionalidade e, conforme as tabelas avançam no fluxo, é definido um schema, até chegar na tabela *gold* que será consumida para *analystics*.

A tabela final possui apenas 3 (três) domínios:

| Variável | Tipo de Dado | Descrição |
| -------- | ------------ | --------- |
| REG_ANS | string | Código ANS de identificação da operadora |
| NOME_FANTASIA | string | Nome fantasia da operadora |
| CUSTO_BENEFICIARIO | double | Custo por beneficiário em reais (R$) por trimestre |
 
## Carga
