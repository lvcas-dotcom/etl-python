import logging
import os
from datetime import datetime
from colorama import Fore, Style, init

# Inicializar colorama
init(autoreset=True)

class ColoredLogger:
    """
    Classe para criar logs coloridos no console e em ficheiros.
    """
    # Cores para diferentes níveis de log
    COLORS = {
        'DEBUG': Fore.BLUE,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.MAGENTA + Style.BRIGHT
    }
    
    def __init__(self, name, log_file=None):
        """
        Inicializa o logger com nome e ficheiro de log opcional.
        
        Args:
            name (str): Nome do logger
            log_file (str, optional): Caminho para o ficheiro de log. Se None, 
                                     será criado um ficheiro com timestamp.
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        
        # Criar formatador para console
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Configurar handler para console com cores
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(self._ColoredFormatter(console_formatter, self.COLORS))
        self.logger.addHandler(console_handler)
        
        # Configurar handler para ficheiro se especificado
        if log_file is None:
            os.makedirs('logs', exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = f"logs/etl_{timestamp}.log"
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        self.log_file = log_file
    
    class _ColoredFormatter(logging.Formatter):
        """
        Formatador personalizado para adicionar cores aos logs no console.
        """
        def __init__(self, formatter, colors):
            self.formatter = formatter
            self.colors = colors
            
        def format(self, record):
            colored_record = logging.makeLogRecord(vars(record))
            levelname = colored_record.levelname
            message = self.formatter.format(colored_record)
            
            if levelname in self.colors:
                message = self.colors[levelname] + message + Style.RESET_ALL
                
            return message
    
    def debug(self, message):
        """Regista uma mensagem de nível DEBUG."""
        self.logger.debug(message)
    
    def info(self, message):
        """Regista uma mensagem de nível INFO."""
        self.logger.info(message)
    
    def warning(self, message):
        """Regista uma mensagem de nível WARNING."""
        self.logger.warning(message)
    
    def error(self, message):
        """Regista uma mensagem de nível ERROR."""
        self.logger.error(message)
    
    def critical(self, message):
        """Regista uma mensagem de nível CRITICAL."""
        self.logger.critical(message)
