import os
import sys
import argparse
import json
import yaml
from typing import Dict, Any, Optional

# Adicionar o diretório src ao path para importações relativas
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import ColoredLogger
from extractor.database_extractor import DatabaseConnector, DataExtractor
from loader.database_loader import DataLoader

class ETLPipeline:
    """
    Classe principal para orquestrar o fluxo ETL completo.
    """
    def __init__(self, config_file: Optional[str] = None):
        """
        Inicializa o pipeline ETL.
        
        Args:
            config_file (str, optional): Caminho para o ficheiro de configuração
        """
        self.logger = ColoredLogger(name="ETL_PIPELINE")
        self.config = {}
        
        if config_file:
            self.load_config(config_file)
    
    def load_config(self, config_file: str) -> bool:
        """
        Carrega a configuração a partir de um ficheiro JSON ou YAML.
        
        Args:
            config_file (str): Caminho para o ficheiro de configuração
            
        Returns:
            bool: True se o carregamento for bem-sucedido, False caso contrário
        """
        if not os.path.exists(config_file):
            self.logger.error(f"Ficheiro de configuração não encontrado: {config_file}")
            return False
        
        try:
            file_ext = os.path.splitext(config_file)[1].lower()
            
            if file_ext == '.json':
                with open(config_file, 'r') as f:
                    self.config = json.load(f)
            elif file_ext in ['.yaml', '.yml']:
                with open(config_file, 'r') as f:
                    self.config = yaml.safe_load(f)
            else:
                self.logger.error(f"Formato de ficheiro não suportado: {file_ext}")
                return False
            
            self.logger.info(f"Configuração carregada com sucesso de {config_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar configuração: {str(e)}")
            return False
    
    def set_config(self, config: Dict[str, Any]) -> None:
        """
        Define a configuração diretamente a partir de um dicionário.
        
        Args:
            config (dict): Configuração do ETL
        """
        self.config = config
        self.logger.info("Configuração definida manualmente")
    
    def validate_config(self) -> bool:
        """
        Valida a configuração do ETL.
        
        Returns:
            bool: True se a configuração for válida, False caso contrário
        """
        required_keys = ['source_db', 'target_db', 'extraction', 'loading']
        
        for key in required_keys:
            if key not in self.config:
                self.logger.error(f"Configuração inválida: chave '{key}' não encontrada")
                return False
        
        # Validar configuração da base de dados de origem
        source_db = self.config['source_db']
        if 'type' not in source_db or 'connection_params' not in source_db:
            self.logger.error("Configuração inválida para base de dados de origem")
            return False
        
        # Validar configuração da base de dados de destino
        target_db = self.config['target_db']
        if 'type' not in target_db or 'connection_params' not in target_db:
            self.logger.error("Configuração inválida para base de dados de destino")
            return False
        
        # Validar configuração de extração
        extraction = self.config['extraction']
        if 'query' not in extraction:
            self.logger.error("Configuração inválida para extração: query não encontrada")
            return False
        
        # Validar configuração de carregamento
        loading = self.config['loading']
        if 'target_table' not in loading:
            self.logger.error("Configuração inválida para carregamento: target_table não encontrada")
            return False
        
        self.logger.info("Configuração validada com sucesso")
        return True
    
    def run(self) -> bool:
        """
        Executa o pipeline ETL completo.
        
        Returns:
            bool: True se o processo for bem-sucedido, False caso contrário
        """
        if not self.validate_config():
            return False
        
        self.logger.info("Iniciando pipeline ETL")
        
        # Configurar conexão com a base de dados de origem
        source_config = self.config['source_db']
        source_connector = DatabaseConnector(
            db_type=source_config['type'],
            connection_params=source_config['connection_params']
        )
        
        # Configurar conexão com a base de dados de destino
        target_config = self.config['target_db']
        target_connector = DatabaseConnector(
            db_type=target_config['type'],
            connection_params=target_config['connection_params']
        )
        
        # Inicializar extrator
        extractor = DataExtractor(source_connector)
        
        # Inicializar carregador
        loader = DataLoader(target_connector)
        
        # Extrair dados
        extraction_config = self.config['extraction']
        query = extraction_config['query']
        query_params = extraction_config.get('params')
        
        self.logger.info("Iniciando extração de dados")
        if not extractor.extract_data(query, query_params):
            self.logger.error("Falha na extração de dados")
            return False
        
        # Obter dados extraídos
        data, columns = extractor.get_extracted_data()
        if not data or not columns:
            self.logger.error("Nenhum dado extraído")
            return False
        
        self.logger.info(f"Dados extraídos com sucesso: {len(data)} registos")
        
        # Configurar carregamento
        loading_config = self.config['loading']
        target_table = loading_config['target_table']
        target_columns = loading_config.get('target_columns')
        column_mapping = loading_config.get('column_mapping')
        truncate_before_load = loading_config.get('truncate_before_load', False)
        create_table = loading_config.get('create_table', False)
        
        # Criar tabela se necessário
        if create_table and 'column_definitions' in loading_config:
            column_definitions = loading_config['column_definitions']
            if not loader.create_table_if_not_exists(target_table, column_definitions):
                self.logger.error("Falha ao criar tabela de destino")
                return False
        
        # Limpar tabela se necessário
        if truncate_before_load:
            if not loader.truncate_table(target_table):
                self.logger.error("Falha ao limpar tabela de destino")
                return False
        
        # Carregar dados
        self.logger.info("Iniciando carregamento de dados")
        if not loader.load_data(
            data=data,
            source_columns=columns,
            target_table=target_table,
            target_columns=target_columns,
            column_mapping=column_mapping,
            batch_size=loading_config.get('batch_size', 100)
        ):
            self.logger.error("Falha no carregamento de dados")
            return False
        
        # Fechar conexões
        source_connector.disconnect()
        target_connector.disconnect()
        
        self.logger.info("Pipeline ETL concluído com sucesso")
        return True


def main():
    """
    Função principal para executar o ETL a partir da linha de comandos.
    """
    parser = argparse.ArgumentParser(description='ETL Pipeline')
    parser.add_argument('--config', '-c', required=True, help='Caminho para o ficheiro de configuração (JSON ou YAML)')
    args = parser.parse_args()
    
    # Inicializar e executar o pipeline
    pipeline = ETLPipeline(args.config)
    success = pipeline.run()
    
    # Retornar código de saída apropriado
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
