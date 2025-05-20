import os
import sys
from typing import Dict, Any

# Adicionar o diretório src ao path para importações relativas
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import ColoredLogger
from etl_pipeline import ETLPipeline

def test_sqlite_to_sqlite():
    """
    Teste simples de ETL de SQLite para SQLite.
    """
    logger = ColoredLogger(name="TEST_SQLITE")
    logger.info("Iniciando teste de SQLite para SQLite")
    
    # Criar base de dados de origem
    source_db_path = "test_source.db"
    if os.path.exists(source_db_path):
        os.remove(source_db_path)
    
    # Criar base de dados de destino
    target_db_path = "test_target.db"
    if os.path.exists(target_db_path):
        os.remove(target_db_path)
    
    # Configurar conexão com a base de dados de origem
    source_config = {
        'type': 'sqlite',
        'connection_params': {
            'database': source_db_path
        }
    }
    
    # Configurar conexão com a base de dados de destino
    target_config = {
        'type': 'sqlite',
        'connection_params': {
            'database': target_db_path
        }
    }
    
    # Criar tabela de origem e inserir dados de teste
    from extractor.database_extractor import DatabaseConnector
    source_connector = DatabaseConnector(
        db_type=source_config['type'],
        connection_params=source_config['connection_params']
    )
    
    if not source_connector.connect():
        logger.error("Falha ao conectar à base de dados de origem")
        return False
    
    # Criar tabela de teste
    create_table_query = """
    CREATE TABLE IF NOT EXISTS clientes (
        id INTEGER PRIMARY KEY,
        nome TEXT NOT NULL,
        email TEXT,
        idade INTEGER,
        data_registo TEXT
    )
    """
    source_connector.execute_query(create_table_query)
    
    # Inserir dados de teste
    insert_data_query = """
    INSERT INTO clientes (nome, email, idade, data_registo)
    VALUES 
        ('João Silva', 'joao@example.com', 35, '2023-01-15'),
        ('Maria Santos', 'maria@example.com', 28, '2023-02-20'),
        ('Pedro Oliveira', 'pedro@example.com', 42, '2023-03-10'),
        ('Ana Pereira', 'ana@example.com', 31, '2023-04-05'),
        ('Carlos Ferreira', 'carlos@example.com', 39, '2023-05-12')
    """
    source_connector.execute_query(insert_data_query)
    
    # Configurar ETL
    etl_config: Dict[str, Any] = {
        'source_db': source_config,
        'target_db': target_config,
        'extraction': {
            'query': 'SELECT id, nome, email, idade FROM clientes WHERE idade > 30'
        },
        'loading': {
            'target_table': 'clientes_filtrados',
            'create_table': True,
            'column_definitions': {
                'id': 'INTEGER PRIMARY KEY',
                'nome': 'TEXT NOT NULL',
                'email': 'TEXT',
                'idade': 'INTEGER'
            },
            'truncate_before_load': True
        }
    }
    
    # Executar pipeline ETL
    pipeline = ETLPipeline()
    pipeline.set_config(etl_config)
    
    success = pipeline.run()
    
    if success:
        logger.info("Teste concluído com sucesso")
        
        # Verificar dados carregados
        target_connector = DatabaseConnector(
            db_type=target_config['type'],
            connection_params=target_config['connection_params']
        )
        
        if target_connector.connect():
            results = target_connector.execute_query("SELECT * FROM clientes_filtrados")
            logger.info(f"Dados carregados na tabela de destino: {len(results)} registos")
            
            for row in results:
                logger.info(f"Registo: {row}")
            
            target_connector.disconnect()
    else:
        logger.error("Teste falhou")
    
    # Limpar
    source_connector.disconnect()
    
    return success


if __name__ == '__main__':
    test_sqlite_to_sqlite()
