## Aplicação de full load dos dados do Databricks para o Metabase

A nossa pipeline de dados no Databricks carrega os dados da camada gold em um bucket S3 em formato `parquet`.

No entanto, esse ambiente não permite analytics, como o Databricks ou alguma outra plataforma full-service. Por isso, essa aplicação extrai os dados do S3 e carrega em uma banco de dados (SQLite) para ser usado pelo Metabase.

Essa é a etapa final do nosso pipeline ETL. Esse processo permite persistir os dados, após encerrar o workspace do Databricks, e extrair insigths a partir das tabelas geradas.

### Iniciando a aplicação

A primeira coisa que precisa ser feita é configurar um arquivo `.env` na raiz do projeto com as credenciais necessárias para a AWS permitir que nossa aplicação leia os dados do bucket no S3.

```shell
AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

Após definir as variáveis de ambiente, configuramos um arquivo `docker-compose.yml` com as instruções para criação dos nossos serviços:

- Metabase: container disponibilizando o serviço na porta 3000;
- Aplicação: executa o script `main.py` que lê os dados do S3 e faz o full load das tabelas no banco de dados.

A aplicação utiliza o SQLite, mas pode ser utilizada qualquer outro DBMS ou serviços como BigQuery e Amazon Athena.

```yaml
services:
  metabase:
    image: metabase/metabase
    container_name: metabase
    ports:
      - "3000:3000"
    restart: 'always'
    volumes:
      - ./app/:/home/metabase/:rw

  full-load:
    build:
      context: ./app
      dockerfile: docker/Dockerfile
    container_name: full-load
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
    volumes:
      - ./app/:/code/:rw
```

O arquivo do banco de dados é salvo dentro do container do Metabase, em `/home/metabase`, através dos `volumes`.

Para iniciar todos os containers da aplicação:

```shell
docker compose up -d
```

E para deletar todos os container, caso deseje fazer alterações:

```shell
docker compose down
```

O serviço de `full-load` pode ser executado de forma independente para executar a pipeline por acionamento manual ou de algum scheduler. Isso faz com que as tabelas sejam atualizadas com os dados mais recentes. 

```shell
docker compose start full-load
```

### Configurações da aplicação

O arquivo `settings.py` permite algumas configurações, como a região da AWS, o nome do bucket no S3, as tabelas que deseja carregar no banco e o nome do banco de dados.

```python
import os

DATABASE = 'databricks'

AWS_CREDENTIALS = {
    'aws_access_key_id': os.environ['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key': os.environ['AWS_SECRET_ACCESS_KEY'],
    'region_name': 'us-east-2'
}

BUCKET = 'databricks-gold-layer'

S3_OBJECT_KEYS = {
    'sinistralidade': 'sinistralidade.parquet'
}
```

### Metabase

O Metabase é uma ferramenta de BI de código aberto, que permite que os usuários paguem por uma solução 'administrada' ou gerenciem sua própria infraestrutura.

Nesse projeto o Metabase está sendo hospedado em uma instância EC2, mas pode ser hospedado localmente também.

Após fazer toda configuração da aplicação e inicar os containers, você pode acessar `localhost:3000` no navegador, ou `<ENDERECO_IP_SERVIDOR>:3000` se estiver usando servidores remotos, para configurar o Metabase e começar a criar consultar e visualizações.  
