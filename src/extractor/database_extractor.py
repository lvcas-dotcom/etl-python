import os
import sys
import sqlite3
import mysql.connector
import psycopg2
from typing import Dict, List, Tuple, Any, Optional, Union

# Adicionar o diretório src ao path para importações relativas
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import ColoredLogger

class DatabaseConnector:
    """
    Classe para gerir conexões com diferentes tipos de bases de dados.
    Suporta SQLite, MySQL e PostgreSQL.
    """
    def __init__(self, db_type: str, connection_params: Dict[str, Any]):
        """
        Inicializa o conector de base de dados.
        
        Args:
            db_type (str): Tipo de base de dados ('sqlite', 'mysql', 'postgresql')
            connection_params (dict): Parâmetros de conexão específicos para cada tipo de BD
        """
        self.logger = ColoredLogger(name=f"DB_{db_type.upper()}")
        self.db_type = db_type.lower()
        self.connection_params = connection_params
        self.connection = None
        self.cursor = None
        
    def connect(self) -> bool:
        """
        Estabelece conexão com a base de dados.
        
        Returns:
            bool: True se a conexão for bem-sucedida, False caso contrário
        """
        try:
            if self.db_type == 'sqlite':
                self.connection = sqlite3.connect(self.connection_params.get('database', ':memory:'))
            elif self.db_type == 'mysql':
                self.connection = mysql.connector.connect(
                    host=self.connection_params.get('host', 'localhost'),
                    user=self.connection_params.get('user', 'root'),
                    password=self.connection_params.get('password', ''),
                    database=self.connection_params.get('database', ''),
                    port=self.connection_params.get('port', 3306)
                )
            elif self.db_type == 'postgresql':
                self.connection = psycopg2.connect(
                    host=self.connection_params.get('host', 'localhost'),
                    user=self.connection_params.get('user', 'postgres'),
                    password=self.connection_params.get('password', ''),
                    dbname=self.connection_params.get('database', ''),
                    port=self.connection_params.get('port', 5432)
                )
            else:
                self.logger.error(f"Tipo de base de dados não suportado: {self.db_type}")
                return False
            
            self.cursor = self.connection.cursor()
            self.logger.info(f"Conexão estabelecida com sucesso à base de dados {self.db_type}")
            return True
        
        except Exception as e:
            self.logger.error(f"Erro ao conectar à base de dados {self.db_type}: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """
        Fecha a conexão com a base de dados.
        """
        if self.connection:
            try:
                if self.cursor:
                    self.cursor.close()
                self.connection.close()
                self.logger.info("Conexão fechada com sucesso")
            except Exception as e:
                self.logger.error(f"Erro ao fechar conexão: {str(e)}")
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Optional[List[Tuple]]:
        """
        Executa uma query SQL na base de dados.
        
        Args:
            query (str): Query SQL a ser executada
            params (tuple, optional): Parâmetros para a query
            
        Returns:
            list: Resultados da query ou None em caso de erro
        """
        if not self.connection or not self.cursor:
            self.logger.error("Não há conexão ativa com a base de dados")
            return None
        
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            # Se for uma query SELECT, retorna os resultados
            if query.strip().upper().startswith("SELECT"):
                results = self.cursor.fetchall()
                self.logger.info(f"Query executada com sucesso: {len(results)} registos encontrados")
                return results
            else:
                # Para queries de modificação (INSERT, UPDATE, DELETE)
                self.connection.commit()
                self.logger.info(f"Query executada com sucesso: {self.cursor.rowcount} registos afetados")
                return []
        
        except Exception as e:
            self.logger.error(f"Erro ao executar query: {str(e)}")
            if not query.strip().upper().startswith("SELECT"):
                self.logger.info("A tentar fazer rollback da transação")
                try:
                    self.connection.rollback()
                except Exception as rollback_error:
                    self.logger.error(f"Erro ao fazer rollback: {str(rollback_error)}")
            return None
    
    def get_column_names(self) -> Optional[List[str]]:
        """
        Obtém os nomes das colunas da última query executada.
        
        Returns:
            list: Lista com os nomes das colunas ou None em caso de erro
        """
        if not self.cursor:
            self.logger.error("Não há cursor ativo")
            return None
        
        try:
            if self.db_type == 'sqlite':
                return [desc[0] for desc in self.cursor.description]
            elif self.db_type == 'mysql':
                return [desc[0] for desc in self.cursor.description]
            elif self.db_type == 'postgresql':
                return [desc[0] for desc in self.cursor.description]
            return None
        except Exception as e:
            self.logger.error(f"Erro ao obter nomes das colunas: {str(e)}")
            return None


class DataExtractor:
    """
    Classe para extrair dados de uma base de dados usando uma query SELECT personalizada.
    """
    def __init__(self, db_connector: DatabaseConnector):
        """
        Inicializa o extrator de dados.
        
        Args:
            db_connector (DatabaseConnector): Conector para a base de dados de origem
        """
        self.db_connector = db_connector
        self.logger = ColoredLogger(name="EXTRACTOR")
        self.data = None
        self.columns = None
    
    def extract_data(self, query: str, params: Optional[Tuple] = None) -> bool:
        """
        Extrai dados da base de dados usando uma query SELECT personalizada.
        
        Args:
            query (str): Query SQL SELECT para extrair os dados
            params (tuple, optional): Parâmetros para a query
            
        Returns:
            bool: True se a extração for bem-sucedida, False caso contrário
        """
        if not query.strip().upper().startswith("SELECT"):
            self.logger.error("A query deve começar com SELECT")
            return False
        
        self.logger.info(f"A extrair dados com a query: {query}")
        
        # Conectar à base de dados se ainda não estiver conectado
        if not self.db_connector.connection:
            if not self.db_connector.connect():
                return False
        
        # Executar a query
        self.data = self.db_connector.execute_query(query, params)
        if self.data is None:
            return False
        
        # Obter os nomes das colunas
        self.columns = self.db_connector.get_column_names()
        if not self.columns:
            self.logger.warning("Não foi possível obter os nomes das colunas")
            return False
        
        self.logger.info(f"Extração concluída com sucesso: {len(self.data)} registos extraídos")
        self.logger.info(f"Colunas extraídas: {', '.join(self.columns)}")
        return True
    
    def get_extracted_data(self) -> Tuple[Optional[List[Tuple]], Optional[List[str]]]:
        """
        Retorna os dados extraídos e os nomes das colunas.
        
        Returns:
            tuple: (dados, colunas) ou (None, None) se não houver dados
        """
        return self.data, self.columns
