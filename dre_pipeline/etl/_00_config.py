"""
DRE Analytics 2025 - Pipeline ETL
Módulo de Configuração

Carrega e valida configurações do arquivo config.yml
"""

import os
import sys
import hashlib
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# =============================================================================
# CONSTANTES
# =============================================================================

# Diretório raiz do projeto (um nível acima de /etl)
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# Caminhos padrão
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yml"

# Chaves obrigatórias no config
REQUIRED_DATABASE_KEYS = ["host", "port", "name", "user", "password"]
REQUIRED_ETL_KEYS = ["source_file", "sheets", "ano_referencia"]


# =============================================================================
# EXCEÇÕES
# =============================================================================

class ConfigError(Exception):
    """Erro de configuração"""
    pass


class ConfigNotFoundError(ConfigError):
    """Arquivo de configuração não encontrado"""
    pass


class ConfigValidationError(ConfigError):
    """Erro de validação do arquivo de configuração"""
    pass


# =============================================================================
# CLASSE PRINCIPAL
# =============================================================================

class Config:
    """
    Gerenciador de configuração do projeto.
    
    Uso:
        config = Config()  # Carrega config.yml automaticamente
        engine = config.get_engine()
        params = config.get_etl_params()
    """
    
    _instance: Optional['Config'] = None
    _config: Dict[str, Any] = {}
    _engine: Optional[Engine] = None
    
    def __new__(cls, config_path: Optional[Path] = None):
        """Singleton pattern para garantir única instância"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Inicializa a configuração.
        
        Args:
            config_path: Caminho para o arquivo de configuração.
                        Se None, usa config.yml no diretório raiz.
        """
        if self._initialized:
            return
            
        self._config_path = config_path or DEFAULT_CONFIG_PATH
        self._load_config()
        self._validate_config()
        self._initialized = True
    
    def _load_config(self) -> None:
        """Carrega o arquivo de configuração YAML"""
        if not self._config_path.exists():
            raise ConfigNotFoundError(
                f"Arquivo de configuração não encontrado: {self._config_path}\n"
                f"Crie o arquivo config.yml com suas configurações."
            )
        
        try:
            with open(self._config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigValidationError(f"Erro ao parsear YAML: {e}")
        except Exception as e:
            raise ConfigError(f"Erro ao carregar configuração: {e}")
    
    def _validate_config(self) -> None:
        """Valida as configurações obrigatórias"""
        errors = []
        
        # Validar seção database
        if "database" not in self._config:
            errors.append("Seção 'database' não encontrada")
        else:
            db = self._config["database"]
            for key in REQUIRED_DATABASE_KEYS:
                if key not in db or db[key] is None:
                    errors.append(f"database.{key} é obrigatório")
        
        # Validar seção etl
        if "etl" not in self._config:
            errors.append("Seção 'etl' não encontrada")
        else:
            etl = self._config["etl"]
            for key in REQUIRED_ETL_KEYS:
                if key not in etl or etl[key] is None:
                    errors.append(f"etl.{key} é obrigatório")
        
        if errors:
            raise ConfigValidationError(
                "Erros de validação no config.yml:\n" + 
                "\n".join(f"  - {e}" for e in errors)
            )
    
    # =========================================================================
    # PROPRIEDADES E GETTERS
    # =========================================================================
    
    @property
    def raw(self) -> Dict[str, Any]:
        """Retorna o dicionário de configuração completo"""
        return self._config
    
    @property
    def project_root(self) -> Path:
        """Retorna o diretório raiz do projeto"""
        return PROJECT_ROOT
    
    def get(self, key: str, default: Any = None) -> Any:
        """Obtém um valor de configuração"""
        return self._config.get(key, default)
    
    def get_database_config(self) -> Dict[str, Any]:
        """Retorna configurações do banco de dados"""
        return self._config.get("database", {})
    
    def get_etl_config(self) -> Dict[str, Any]:
        """Retorna configurações do ETL"""
        return self._config.get("etl", {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Retorna configurações de logging"""
        return self._config.get("logging", {"level": "INFO", "folder": "logs"})
    
    def get_dq_config(self) -> Dict[str, Any]:
        """Retorna configurações de Data Quality"""
        return self._config.get("dq", {})
    
    def get_output_config(self) -> Dict[str, Any]:
        """Retorna configurações de output"""
        return self._config.get("output", {})
    
    def get_source_file_path(self) -> Path:
        """Retorna o caminho completo do arquivo fonte"""
        etl_config = self.get_etl_config()
        source_file = etl_config.get("source_file", "")
        
        # Se caminho relativo, resolver a partir do PROJECT_ROOT
        source_path = Path(source_file)
        if not source_path.is_absolute():
            source_path = PROJECT_ROOT / source_file
        
        return source_path
    
    def get_output_folder(self) -> Path:
        """Retorna o caminho da pasta de output"""
        output_config = self.get_output_config()
        folder = output_config.get("folder", "../02_dados_tratados/powerbi_model")
        
        output_path = Path(folder)
        if not output_path.is_absolute():
            output_path = PROJECT_ROOT / folder
        
        return output_path
    
    # =========================================================================
    # DATABASE ENGINE
    # =========================================================================
    
    def get_connection_string(self) -> str:
        """Retorna a string de conexão do PostgreSQL"""
        db = self.get_database_config()
        return (
            f"postgresql://{db['user']}:{db['password']}"
            f"@{db['host']}:{db['port']}/{db['name']}"
        )
    
    def get_engine(self) -> Engine:
        """
        Retorna Engine SQLAlchemy (singleton).
        
        Returns:
            Engine SQLAlchemy configurado
        """
        if self._engine is None:
            conn_string = self.get_connection_string()
            self._engine = create_engine(
                conn_string,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True
            )
        return self._engine
    
    def test_connection(self) -> bool:
        """
        Testa a conexão com o banco de dados.
        
        Returns:
            True se conexão bem sucedida
        """
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"❌ Erro de conexão: {e}")
            return False


# =============================================================================
# FUNÇÕES DE CONVENIÊNCIA
# =============================================================================

def get_config(config_path: Optional[Path] = None) -> Config:
    """
    Retorna instância singleton do Config.
    
    Args:
        config_path: Caminho opcional para o arquivo de configuração
        
    Returns:
        Instância do Config
    """
    return Config(config_path)


def get_engine() -> Engine:
    """
    Retorna Engine SQLAlchemy.
    
    Returns:
        Engine SQLAlchemy configurado
    """
    return get_config().get_engine()


def test_connection() -> bool:
    """
    Testa a conexão com o banco de dados.
    
    Returns:
        True se conexão bem sucedida
    """
    return get_config().test_connection()


def generate_batch_id() -> str:
    """
    Gera um ID único para o batch de ingestão.
    
    Returns:
        String com ID único baseado em timestamp + hash
    """
    import time
    timestamp = str(time.time())
    return hashlib.md5(timestamp.encode()).hexdigest()[:12]


# =============================================================================
# MAPEAMENTOS DE MESES
# =============================================================================

MESES_MAP = {
    'JAN': {'num': 1, 'nome_completo': 'Janeiro', 'trimestre': 'Q1', 'semestre': 'S1'},
    'FEV': {'num': 2, 'nome_completo': 'Fevereiro', 'trimestre': 'Q1', 'semestre': 'S1'},
    'MAR': {'num': 3, 'nome_completo': 'Março', 'trimestre': 'Q1', 'semestre': 'S1'},
    'ABR': {'num': 4, 'nome_completo': 'Abril', 'trimestre': 'Q2', 'semestre': 'S1'},
    'MAI': {'num': 5, 'nome_completo': 'Maio', 'trimestre': 'Q2', 'semestre': 'S1'},
    'JUN': {'num': 6, 'nome_completo': 'Junho', 'trimestre': 'Q2', 'semestre': 'S1'},
    'JUL': {'num': 7, 'nome_completo': 'Julho', 'trimestre': 'Q3', 'semestre': 'S2'},
    'AGO': {'num': 8, 'nome_completo': 'Agosto', 'trimestre': 'Q3', 'semestre': 'S2'},
    'SET': {'num': 9, 'nome_completo': 'Setembro', 'trimestre': 'Q3', 'semestre': 'S2'},
    'OUT': {'num': 10, 'nome_completo': 'Outubro', 'trimestre': 'Q4', 'semestre': 'S2'},
    'NOV': {'num': 11, 'nome_completo': 'Novembro', 'trimestre': 'Q4', 'semestre': 'S2'},
    'DEZ': {'num': 12, 'nome_completo': 'Dezembro', 'trimestre': 'Q4', 'semestre': 'S2'},
}


def get_mes_info(mes_nome: str) -> Dict[str, Any]:
    """Retorna informações de um mês pelo nome abreviado"""
    return MESES_MAP.get(mes_nome.upper(), {})


def get_data_key(ano: int, mes_num: int) -> str:
    """Gera a data_key no formato YYYY-MM"""
    return f"{ano}-{mes_num:02d}"


# =============================================================================
# MAIN - PARA TESTES
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("   DRE Analytics 2025 - Teste de Configuração")
    print("=" * 60)
    
    try:
        config = get_config()
        print(f"✅ Configuração carregada de: {config._config_path}")
        print(f"📁 Diretório raiz: {config.project_root}")
        print(f"📄 Arquivo fonte: {config.get_source_file_path()}")
        print(f"📂 Pasta output: {config.get_output_folder()}")
        
        print("\n🔌 Testando conexão com banco...")
        if config.test_connection():
            print("✅ Conexão bem sucedida!")
        else:
            print("❌ Falha na conexão")
            
    except ConfigError as e:
        print(f"❌ Erro de configuração: {e}")
        sys.exit(1)
