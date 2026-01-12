"""
DRE Analytics 2025 - Pipeline ETL
Transformação STG → DW

Popula as tabelas dimensão e fato do Star Schema.
"""

import logging
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ._00_config import get_engine


# =============================================================================
# CONFIGURAÇÃO DE LOGGING
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# FUNÇÕES DE CARGA - DIMENSÕES
# =============================================================================

def load_dim_unidade(engine: Engine) -> int:
    """
    Carrega stg.despesa → dw.dim_unidade
    
    Extrai unidades únicas das despesas e receitas.
    """
    print("\n📊 Carregando dimensão: dim_unidade")
    
    query = """
        INSERT INTO dw.dim_unidade (unidade, is_active, dw_loaded_at)
        SELECT DISTINCT unidade, true, NOW()
        FROM (
            SELECT DISTINCT unidade FROM stg.receita
            UNION
            SELECT DISTINCT unidade FROM stg.despesa
        ) u
        WHERE unidade IS NOT NULL
          AND TRIM(unidade) != ''
        ON CONFLICT (unidade) DO NOTHING
    """
    
    with engine.connect() as conn:
        # Não truncar - usar UPSERT para manter histórico
        result = conn.execute(text(query))
        conn.commit()
        rows = result.rowcount
    
    logger.info(f"   ✅ Carregadas {rows} unidades em dw.dim_unidade")
    return rows


def load_dim_pacote(engine: Engine) -> int:
    """
    Carrega stg.despesa → dw.dim_pacote
    
    Extrai pacotes únicos das despesas.
    """
    print("\n📊 Carregando dimensão: dim_pacote")
    
    query = """
        INSERT INTO dw.dim_pacote (pacote, is_active, dw_loaded_at)
        SELECT DISTINCT pacote, true, NOW()
        FROM stg.despesa
        WHERE pacote IS NOT NULL
          AND TRIM(pacote) != ''
        ON CONFLICT (pacote) DO NOTHING
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        conn.commit()
        rows = result.rowcount
    
    logger.info(f"   ✅ Carregados {rows} pacotes em dw.dim_pacote")
    return rows


def load_dim_linha_dre(engine: Engine) -> int:
    """
    Carrega stg.dre → dw.dim_linha_dre
    
    Extrai linhas DRE únicas com hierarquia.
    """
    print("\n📊 Carregando dimensão: dim_linha_dre")
    
    query = """
        INSERT INTO dw.dim_linha_dre (linha_dre, categoria, ordem, nivel, is_total, dw_loaded_at)
        SELECT DISTINCT
            linha_dre,
            categoria,
            ordem,
            nivel,
            CASE 
                WHEN UPPER(linha_dre) IN ('RECEITA LÍQUIDA', 'EBITDA META', 'EBITDA', 'EBT', 'LUCRO LÍQUIDO')
                THEN true
                ELSE false
            END as is_total,
            NOW()
        FROM stg.dre
        WHERE linha_dre IS NOT NULL
        ON CONFLICT (linha_dre) DO UPDATE SET
            categoria = EXCLUDED.categoria,
            ordem = EXCLUDED.ordem,
            nivel = EXCLUDED.nivel,
            is_total = EXCLUDED.is_total,
            dw_loaded_at = NOW()
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        conn.commit()
        rows = result.rowcount
    
    logger.info(f"   ✅ Carregadas {rows} linhas DRE em dw.dim_linha_dre")
    return rows


# =============================================================================
# FUNÇÕES DE CARGA - FATOS
# =============================================================================

def load_fact_receita(engine: Engine) -> int:
    """
    Carrega stg.receita → dw.fact_receita
    """
    print("\n📈 Carregando fato: fact_receita")
    
    query = """
        INSERT INTO dw.fact_receita (
            data_key, cenario, tipo_receita, unidade, valor, dw_loaded_at
        )
        SELECT
            data_key,
            cenario,
            tipo_receita,
            unidade,
            valor,
            NOW()
        FROM stg.receita
    """
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dw.fact_receita"))
        result = conn.execute(text(query))
        conn.commit()
        rows = result.rowcount
    
    logger.info(f"   ✅ Carregados {rows} registros em dw.fact_receita")
    return rows


def load_fact_despesa(engine: Engine) -> int:
    """
    Carrega stg.despesa → dw.fact_despesa
    """
    print("\n📈 Carregando fato: fact_despesa")
    
    query = """
        INSERT INTO dw.fact_despesa (
            data_key, cenario, unidade, pacote, conta, valor, dw_loaded_at
        )
        SELECT
            data_key,
            cenario,
            unidade,
            pacote,
            conta,
            valor,
            NOW()
        FROM stg.despesa
    """
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dw.fact_despesa"))
        result = conn.execute(text(query))
        conn.commit()
        rows = result.rowcount
    
    logger.info(f"   ✅ Carregados {rows} registros em dw.fact_despesa")
    return rows


def load_fact_dre(engine: Engine) -> int:
    """
    Carrega stg.dre → dw.fact_dre
    """
    print("\n📈 Carregando fato: fact_dre")
    
    query = """
        INSERT INTO dw.fact_dre (
            data_key, cenario, linha_dre, valor, dw_loaded_at
        )
        SELECT
            data_key,
            'Realizado' as cenario,
            linha_dre,
            valor,
            NOW()
        FROM stg.dre
    """
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dw.fact_dre"))
        result = conn.execute(text(query))
        conn.commit()
        rows = result.rowcount
    
    logger.info(f"   ✅ Carregados {rows} registros em dw.fact_dre")
    return rows


def load_fact_aliquota(engine: Engine) -> int:
    """
    Carrega stg.aliquota → dw.fact_aliquota
    """
    print("\n📈 Carregando fato: fact_aliquota")
    
    query = """
        INSERT INTO dw.fact_aliquota (
            data_key, tipo_imposto, aliquota, dw_loaded_at
        )
        SELECT
            data_key,
            tipo_imposto,
            aliquota,
            NOW()
        FROM stg.aliquota
    """
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dw.fact_aliquota"))
        result = conn.execute(text(query))
        conn.commit()
        rows = result.rowcount
    
    logger.info(f"   ✅ Carregados {rows} registros em dw.fact_aliquota")
    return rows


# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def run_transform_stg_to_dw(engine: Optional[Engine] = None) -> Dict[str, int]:
    """
    Executa todas as transformações STG → DW.
    
    Args:
        engine: Engine SQLAlchemy (opcional)
        
    Returns:
        Dicionário com contagem de registros por tabela
    """
    print("\n" + "=" * 60)
    print("   TRANSFORMAÇÃO: STG → DW (Gold)")
    print("=" * 60)
    
    engine = engine or get_engine()
    
    # Carregar dimensões primeiro
    print("\n--- Dimensões ---")
    dim_results = {
        'dim_unidade': load_dim_unidade(engine),
        'dim_pacote': load_dim_pacote(engine),
        'dim_linha_dre': load_dim_linha_dre(engine)
    }
    
    # Depois carregar fatos
    print("\n--- Fatos ---")
    fact_results = {
        'fact_receita': load_fact_receita(engine),
        'fact_despesa': load_fact_despesa(engine),
        'fact_dre': load_fact_dre(engine),
        'fact_aliquota': load_fact_aliquota(engine)
    }
    
    results = {**dim_results, **fact_results}
    total = sum(results.values())
    
    print(f"\n✅ Transformação STG→DW completa: {total} registros")
    
    return results


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    run_transform_stg_to_dw()
