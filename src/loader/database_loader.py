import os
import sys
from typing import Dict, List, Tuple, Any, Optional, Union

# Adicionar o diretório src ao path para importações relativas
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import ColoredLogger
from extractor.database_extractor import DatabaseConnector

class DataLoader:
    """
    Classe para carregar dados numa base de dados de destino com mapeamento flexível de campos.
    """
    def __init__(self, db_connector: DatabaseConnector):
        """
        Inicializa o carregador de dados.
        
        Args:
            db_connector (DatabaseConnector): Conector para a base de dados de destino
        """
        self.db_connector = db_connector
        self.logger = ColoredLogger(name="LOADER")
    
    def load_data(self, 
                  data: List[Tuple], 
                  source_columns: List[str], 
                  target_table: str, 
                  target_columns: Optional[List[str]] = None,
                  column_mapping: Optional[Dict[str, str]] = None,
                  batch_size: int = 100) -> bool:
        """
        Carrega dados na tabela de destino com mapeamento flexível de colunas.
        
        Args:
            data (list): Dados a serem carregados (lista de tuplos)
            source_columns (list): Nomes das colunas de origem
            target_table (str): Nome da tabela de destino
            target_columns (list, optional): Nomes das colunas de destino. 
                                           Se None, usa os mesmos nomes das colunas de origem.
            column_mapping (dict, optional): Mapeamento personalizado de colunas {origem: destino}.
                                           Se fornecido, sobrepõe-se a target_columns.
            batch_size (int): Tamanho do lote para inserções em massa
            
        Returns:
            bool: True se o carregamento for bem-sucedido, False caso contrário
        """
        if not data or not source_columns:
            self.logger.error("Dados ou colunas de origem vazios")
            return False
        
        # Conectar à base de dados se ainda não estiver conectado
        if not self.db_connector.connection:
            if not self.db_connector.connect():
                return False
        
        # Determinar as colunas de destino com base nos parâmetros
        if column_mapping:
            # Usar mapeamento personalizado
            dest_columns = []
            source_indices = []
            
            for i, src_col in enumerate(source_columns):
                if src_col in column_mapping:
                    dest_columns.append(column_mapping[src_col])
                    source_indices.append(i)
            
            if not dest_columns:
                self.logger.error("Nenhuma coluna válida encontrada no mapeamento")
                return False
            
            # Filtrar os dados para incluir apenas as colunas mapeadas
            filtered_data = []
            for row in data:
                filtered_row = tuple(row[i] for i in source_indices)
                filtered_data.append(filtered_row)
            
            data_to_insert = filtered_data
            
        elif target_columns:
            # Usar colunas de destino especificadas
            if len(target_columns) != len(source_columns):
                self.logger.error(f"Número de colunas não corresponde: origem ({len(source_columns)}) vs destino ({len(target_columns)})")
                return False
            
            dest_columns = target_columns
            data_to_insert = data
            
        else:
            # Usar os mesmos nomes de colunas da origem
            dest_columns = source_columns
            data_to_insert = data
        
        # Construir a query de inserção
        placeholders = ", ".join(["?"] * len(dest_columns))
        columns_str = ", ".join([f'"{col}"' for col in dest_columns])
        
        # Ajustar placeholders conforme o tipo de base de dados
        if self.db_connector.db_type == 'postgresql':
            placeholders = ", ".join([f"%s"] * len(dest_columns))
        
        insert_query = f'INSERT INTO "{target_table}" ({columns_str}) VALUES ({placeholders})'
        
        self.logger.info(f"A carregar {len(data_to_insert)} registos na tabela {target_table}")
        self.logger.info(f"Colunas de destino: {', '.join(dest_columns)}")
        
        # Inserir dados em lotes
        try:
            total_rows = len(data_to_insert)
            for i in range(0, total_rows, batch_size):
                batch = data_to_insert[i:i+batch_size]
                
                # Executar inserção em lote
                cursor = self.db_connector.cursor
                cursor.executemany(insert_query, batch)
                self.db_connector.connection.commit()
                
                self.logger.info(f"Lote {i//batch_size + 1} inserido: {len(batch)} registos")
            
            self.logger.info(f"Carregamento concluído com sucesso: {total_rows} registos inseridos na tabela {target_table}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao inserir dados: {str(e)}")
            try:
                self.db_connector.connection.rollback()
                self.logger.info("Rollback da transação realizado")
            except Exception as rollback_error:
                self.logger.error(f"Erro ao fazer rollback: {str(rollback_error)}")
            return False
    
    def create_table_if_not_exists(self, 
                                  target_table: str, 
                                  column_definitions: Dict[str, str]) -> bool:
        """
        Cria a tabela de destino se não existir.
        
        Args:
            target_table (str): Nome da tabela a criar
            column_definitions (dict): Definições das colunas {nome_coluna: tipo_dados}
            
        Returns:
            bool: True se a operação for bem-sucedida, False caso contrário
        """
        if not column_definitions:
            self.logger.error("Definições de colunas vazias")
            return False
        
        # Conectar à base de dados se ainda não estiver conectado
        if not self.db_connector.connection:
            if not self.db_connector.connect():
                return False
        
        # Construir a definição das colunas
        columns_def = ", ".join([f'"{col}" {dtype}' for col, dtype in column_definitions.items()])
        
        # Construir a query CREATE TABLE
        create_query = f'CREATE TABLE IF NOT EXISTS "{target_table}" ({columns_def})'
        
        self.logger.info(f"A criar tabela {target_table} se não existir")
        
        try:
            # Executar a query
            self.db_connector.execute_query(create_query)
            self.logger.info(f"Tabela {target_table} verificada/criada com sucesso")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao criar tabela: {str(e)}")
            return False
    
    def truncate_table(self, target_table: str) -> bool:
        """
        Limpa todos os dados da tabela de destino.
        
        Args:
            target_table (str): Nome da tabela a limpar
            
        Returns:
            bool: True se a operação for bem-sucedida, False caso contrário
        """
        # Conectar à base de dados se ainda não estiver conectado
        if not self.db_connector.connection:
            if not self.db_connector.connect():
                return False
        
        truncate_query = f'DELETE FROM "{target_table}"'
        
        self.logger.warning(f"A limpar todos os dados da tabela {target_table}")
        
        try:
            # Executar a query
            self.db_connector.execute_query(truncate_query)
            self.logger.info(f"Tabela {target_table} limpa com sucesso")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao limpar tabela: {str(e)}")
            return False
