"""
DRE Analytics 2025 - Testes do Módulo de Configuração
"""

import pytest
import os
import sys

# Adiciona o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestConfig:
    """Testes para o módulo de configuração"""
    
    def test_config_singleton(self):
        """Verifica se Config é singleton"""
        from etl._00_config import get_config
        
        config1 = get_config()
        config2 = get_config()
        
        assert config1 is config2, "Config deve ser singleton"
    
    def test_config_has_database_section(self):
        """Verifica se configuração tem seção database"""
        from etl._00_config import get_config
        
        config = get_config()
        
        assert 'database' in config._config, "Config deve ter seção 'database'"
        assert 'host' in config._config['database'], "Database deve ter 'host'"
        assert 'port' in config._config['database'], "Database deve ter 'port'"
    
    def test_config_has_paths_section(self):
        """Verifica se configuração tem seção paths"""
        from etl._00_config import get_config
        
        config = get_config()
        
        assert 'paths' in config._config, "Config deve ter seção 'paths'"
    
    def test_meses_map_has_12_months(self):
        """Verifica se MESES_MAP tem 12 meses"""
        from etl._00_config import MESES_MAP
        
        assert len(MESES_MAP) == 12, f"MESES_MAP deve ter 12 meses, tem {len(MESES_MAP)}"
    
    def test_meses_map_structure(self):
        """Verifica estrutura do MESES_MAP"""
        from etl._00_config import MESES_MAP
        
        for mes, info in MESES_MAP.items():
            assert 'mes_num' in info, f"Mês {mes} deve ter 'mes_num'"
            assert 'mes_nome' in info, f"Mês {mes} deve ter 'mes_nome'"
            assert 'trimestre' in info, f"Mês {mes} deve ter 'trimestre'"
            assert 'semestre' in info, f"Mês {mes} deve ter 'semestre'"


class TestDataQualityThresholds:
    """Testes para verificar thresholds de DQ"""
    
    def test_expected_values_exist(self):
        """Verifica se valores esperados estão configurados"""
        from etl._00_config import get_config
        
        config = get_config()
        dq = config.get('data_quality', {})
        expected = dq.get('expected', {})
        
        required_keys = [
            'receita_bruta_realizado',
            'receita_bruta_orcado',
            'despesas_realizado',
            'despesas_orcado',
            'lucro_liquido'
        ]
        
        for key in required_keys:
            assert key in expected, f"Expected deve ter '{key}'"
            assert isinstance(expected[key], (int, float)), f"'{key}' deve ser numérico"


class TestGenerateBatchId:
    """Testes para geração de batch_id"""
    
    def test_batch_id_format(self):
        """Verifica formato do batch_id"""
        from etl._00_config import generate_batch_id
        
        batch_id = generate_batch_id()
        
        # Formato: batch_YYYYMMDD_HHMMSS
        assert batch_id.startswith('batch_'), "batch_id deve começar com 'batch_'"
        parts = batch_id.split('_')
        assert len(parts) == 3, "batch_id deve ter 3 partes separadas por _"
        assert len(parts[1]) == 8, "Parte de data deve ter 8 caracteres (YYYYMMDD)"
        assert len(parts[2]) == 6, "Parte de hora deve ter 6 caracteres (HHMMSS)"
    
    def test_batch_id_unique(self):
        """Verifica se batch_ids são únicos"""
        from etl._00_config import generate_batch_id
        import time
        
        batch1 = generate_batch_id()
        time.sleep(1)
        batch2 = generate_batch_id()
        
        assert batch1 != batch2, "batch_ids consecutivos devem ser diferentes"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
