# ETL Python - Integrações de forma simples e robusta.

Este projeto implementa um ETL (Extract, Transform, Load) em Python que permite extrair dados de uma base de dados e inseri-los noutra de forma simples e intuitiva.

## Características

- Arquitetura modular e bem organizada
- Logs coloridos para melhor rastreabilidade
- Suporte para diferentes tipos de bases de dados (SQLite, MySQL, PostgreSQL)
- Configuração flexível via ficheiros JSON ou YAML
- Mapeamento personalizado de colunas
- Processamento em lotes para melhor desempenho
- Interface intuitiva para definir queries SELECT personalizadas

## Estrutura do Projeto

```
etl_project/
├── src/
│   ├── extractor/         # Módulo de extração de dados
│   ├── loader/            # Módulo de carregamento de dados
│   ├── transformer/       # Módulo para transformações futuras
│   ├── utils/             # Utilitários (logs, etc.)
│   ├── config/            # Configurações
│   └── etl_pipeline.py    # Ponto de entrada principal
├── logs/                  # Diretório para ficheiros de log
├── tests/                 # Testes
└── requirements.txt       # Dependências
```

## Instalação

1. Clone o repositório ou descompacte o ficheiro ZIP
2. Instale as dependências:

```bash
pip install -r requirements.txt
```

## Utilização

### Configuração

Crie um ficheiro de configuração JSON ou YAML com os seguintes parâmetros:

```json
{
    "source_db": {
        "type": "sqlite",
        "connection_params": {
            "database": "origem.db"
        }
    },
    "target_db": {
        "type": "sqlite",
        "connection_params": {
            "database": "destino.db"
        }
    },
    "extraction": {
        "query": "SELECT id, nome, email FROM clientes WHERE status = 'ativo'"
    },
    "loading": {
        "target_table": "clientes_ativos",
        "create_table": true,
        "column_definitions": {
            "id": "INTEGER PRIMARY KEY",
            "nome": "TEXT NOT NULL",
            "email": "TEXT"
        },
        "truncate_before_load": true,
        "batch_size": 100
    }
}
```

### Execução via Linha de Comandos

```bash
python src/etl_pipeline.py --config config_exemplo.json
```

### Execução via Código

```python
from etl_pipeline import ETLPipeline

# Carregar configuração de ficheiro
pipeline = ETLPipeline("config_exemplo.json")
pipeline.run()

# Ou definir configuração diretamente
config = {
    "source_db": {
        "type": "sqlite",
        "connection_params": {
            "database": "origem.db"
        }
    },
    "target_db": {
        "type": "mysql",
        "connection_params": {
            "host": "localhost",
            "user": "root",
            "password": "senha",
            "database": "destino_db"
        }
    },
    "extraction": {
        "query": "SELECT * FROM produtos WHERE preco > 100"
    },
    "loading": {
        "target_table": "produtos_premium",
        "column_mapping": {
            "id": "produto_id",
            "nome": "produto_nome",
            "preco": "produto_preco"
        }
    }
}

pipeline = ETLPipeline()
pipeline.set_config(config)
pipeline.run()
```

## Exemplos

### Mapeamento de Colunas

Você pode mapear colunas da origem para destino com nomes diferentes:

```json
"loading": {
    "target_table": "clientes_destino",
    "column_mapping": {
        "id": "cliente_id",
        "nome": "nome_completo",
        "email": "email_contato"
    }
}
```

### Criação Automática de Tabela

```json
"loading": {
    "target_table": "nova_tabela",
    "create_table": true,
    "column_definitions": {
        "id": "INTEGER PRIMARY KEY",
        "nome": "TEXT NOT NULL",
        "email": "TEXT",
        "idade": "INTEGER"
    }
}
```

## Tipos de Bases de Dados Suportados

### SQLite

```json
{
    "type": "sqlite",
    "connection_params": {
        "database": "caminho/para/arquivo.db"
    }
}
```

### MySQL

```json
{
    "type": "mysql",
    "connection_params": {
        "host": "localhost",
        "user": "usuario",
        "password": "senha",
        "database": "nome_db",
        "port": 3306
    }
}
```

### PostgreSQL

```json
{
    "type": "postgresql",
    "connection_params": {
        "host": "localhost",
        "user": "usuario",
        "password": "senha",
        "database": "nome_db",
        "port": 5432
    }
}
```

## Logs Coloridos

O projeto utiliza logs coloridos para facilitar a visualização do progresso:
- DEBUG: Azul
- INFO: Verde
- WARNING: Amarelo
- ERROR: Vermelho
- CRITICAL: Magenta

Os logs são exibidos no console e também salvos em ficheiros na pasta `logs/`.

## Testes

Para executar os testes:

```bash
python -m tests.test_sqlite
```

## Dependências

- colorama: Para logs coloridos
- mysql-connector-python: Para conexão com MySQL
- psycopg2-binary: Para conexão com PostgreSQL
- pyyaml: Para suporte a ficheiros YAML
