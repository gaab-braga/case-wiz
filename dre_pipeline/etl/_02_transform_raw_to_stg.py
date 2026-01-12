"""
DRE Analytics 2025 - Pipeline ETL
Transformação RAW → STG

Aplica limpeza, padronização e validação nos dados brutos.
"""

import logging
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ._00_config import get_engine, get_config, MESES_MAP, get_data_key


# =============================================================================
# CONFIGURAÇÃO DE LOGGING
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# FUNÇÕES DE TRANSFORMAÇÃO
# =============================================================================

def transform_receita(engine: Engine) -> int:
    """
    Transforma raw.receita → stg.receita
    
    Transformações:
    - Adiciona mes_num e data_key
    - Padroniza cenário e tipo_receita
    - Remove registros inválidos
    """
    print("\n🔄 Transformando receita: RAW → STG")
    
    config = get_config()
    ano = config.get_etl_config().get("ano_referencia", 2025)
    
    query = f"""
        INSERT INTO stg.receita (
            cenario, tipo_receita, unidade, mes_num, mes_nome, data_key, valor,
            raw_created_at, stg_loaded_at
        )
        SELECT
            CASE 
                WHEN UPPER(TRIM(cenario)) LIKE '%REALIZADO%' THEN 'Realizado'
                WHEN UPPER(TRIM(cenario)) LIKE '%OR%ADO%' THEN 'Orçado'
                ELSE TRIM(cenario)
            END as cenario,
            UPPER(TRIM(tipo_receita)) as tipo_receita,
            TRIM(unidade) as unidade,
            CASE UPPER(TRIM(mes))
                WHEN 'JAN' THEN 1 WHEN 'FEV' THEN 2 WHEN 'MAR' THEN 3
                WHEN 'ABR' THEN 4 WHEN 'MAI' THEN 5 WHEN 'JUN' THEN 6
                WHEN 'JUL' THEN 7 WHEN 'AGO' THEN 8 WHEN 'SET' THEN 9
                WHEN 'OUT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEZ' THEN 12
            END as mes_num,
            UPPER(TRIM(mes)) as mes_nome,
            '{ano}-' || LPAD(
                CASE UPPER(TRIM(mes))
                    WHEN 'JAN' THEN '1' WHEN 'FEV' THEN '2' WHEN 'MAR' THEN '3'
                    WHEN 'ABR' THEN '4' WHEN 'MAI' THEN '5' WHEN 'JUN' THEN '6'
                    WHEN 'JUL' THEN '7' WHEN 'AGO' THEN '8' WHEN 'SET' THEN '9'
                    WHEN 'OUT' THEN '10' WHEN 'NOV' THEN '11' WHEN 'DEZ' THEN '12'
                END, 2, '0'
            ) as data_key,
            valor,
            created_at as raw_created_at,
            NOW() as stg_loaded_at
        FROM raw.receita
        WHERE valor IS NOT NULL
          AND valor != 0
          AND mes IS NOT NULL
          AND tipo_receita IS NOT NULL
          AND unidade IS NOT NULL
    """
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE stg.receita"))
        result = conn.execute(text(query))
        conn.commit()
        rows = result.rowcount
    
    logger.info(f"   ✅ Transformados {rows} registros para stg.receita")
    return rows


def transform_despesa(engine: Engine) -> int:
    """
    Transforma raw.despesa → stg.despesa
    
    Transformações:
    - Extrai mês da data
    - Adiciona data_key
    - Padroniza campos de texto
    - Remove registros inválidos (valores zerados, sem pacote)
    """
    print("\n🔄 Transformando despesa: RAW → STG")
    
    config = get_config()
    ano = config.get_etl_config().get("ano_referencia", 2025)
    
    query = f"""
        INSERT INTO stg.despesa (
            cenario, unidade, pacote, conta, mes_num, mes_nome, data_key, valor,
            raw_created_at, stg_loaded_at
        )
        SELECT
            CASE 
                WHEN UPPER(TRIM(cenario)) LIKE '%REALIZADO%' THEN 'Realizado'
                WHEN UPPER(TRIM(cenario)) LIKE '%OR%ADO%' THEN 'Orçado'
                ELSE TRIM(cenario)
            END as cenario,
            TRIM(unidade) as unidade,
            TRIM(pacote) as pacote,
            TRIM(conta) as conta,
            EXTRACT(MONTH FROM data)::INTEGER as mes_num,
            CASE EXTRACT(MONTH FROM data)::INTEGER
                WHEN 1 THEN 'JAN' WHEN 2 THEN 'FEV' WHEN 3 THEN 'MAR'
                WHEN 4 THEN 'ABR' WHEN 5 THEN 'MAI' WHEN 6 THEN 'JUN'
                WHEN 7 THEN 'JUL' WHEN 8 THEN 'AGO' WHEN 9 THEN 'SET'
                WHEN 10 THEN 'OUT' WHEN 11 THEN 'NOV' WHEN 12 THEN 'DEZ'
            END as mes_nome,
            '{ano}-' || LPAD(EXTRACT(MONTH FROM data)::TEXT, 2, '0') as data_key,
            valor,
            created_at as raw_created_at,
            NOW() as stg_loaded_at
        FROM raw.despesa
        WHERE valor IS NOT NULL
          AND valor != 0
          AND data IS NOT NULL
          AND pacote IS NOT NULL
          AND TRIM(pacote) != ''
    """
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE stg.despesa"))
        result = conn.execute(text(query))
        conn.commit()
        rows = result.rowcount
    
    logger.info(f"   ✅ Transformados {rows} registros para stg.despesa")
    return rows


def transform_dre(engine: Engine) -> int:
    """
    Transforma raw.dre → stg.dre
    
    Transformações:
    - Adiciona mes_num e data_key
    - Determina nível hierárquico
    - Padroniza categorias
    """
    print("\n🔄 Transformando DRE: RAW → STG")
    
    config = get_config()
    ano = config.get_etl_config().get("ano_referencia", 2025)
    
    query = f"""
        INSERT INTO stg.dre (
            linha_dre, categoria, ordem, nivel, mes_num, mes_nome, data_key, valor,
            raw_created_at, stg_loaded_at
        )
        SELECT
            TRIM(linha_dre) as linha_dre,
            TRIM(categoria) as categoria,
            ordem,
            CASE 
                WHEN UPPER(linha_dre) IN ('RECEITA LÍQUIDA', 'EBITDA META', 'EBITDA', 'EBT', 'LUCRO LÍQUIDO') THEN 1
                WHEN UPPER(linha_dre) IN ('RECEITA BRUTA', 'CUSTOS') THEN 2
                ELSE 3
            END as nivel,
            CASE UPPER(TRIM(mes))
                WHEN 'JAN' THEN 1 WHEN 'FEV' THEN 2 WHEN 'MAR' THEN 3
                WHEN 'ABR' THEN 4 WHEN 'MAI' THEN 5 WHEN 'JUN' THEN 6
                WHEN 'JUL' THEN 7 WHEN 'AGO' THEN 8 WHEN 'SET' THEN 9
                WHEN 'OUT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEZ' THEN 12
            END as mes_num,
            UPPER(TRIM(mes)) as mes_nome,
            '{ano}-' || LPAD(
                CASE UPPER(TRIM(mes))
                    WHEN 'JAN' THEN '1' WHEN 'FEV' THEN '2' WHEN 'MAR' THEN '3'
                    WHEN 'ABR' THEN '4' WHEN 'MAI' THEN '5' WHEN 'JUN' THEN '6'
                    WHEN 'JUL' THEN '7' WHEN 'AGO' THEN '8' WHEN 'SET' THEN '9'
                    WHEN 'OUT' THEN '10' WHEN 'NOV' THEN '11' WHEN 'DEZ' THEN '12'
                END, 2, '0'
            ) as data_key,
            valor,
            created_at as raw_created_at,
            NOW() as stg_loaded_at
        FROM raw.dre
        WHERE valor IS NOT NULL
          AND mes IS NOT NULL
          AND linha_dre IS NOT NULL
    """
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE stg.dre"))
        result = conn.execute(text(query))
        conn.commit()
        rows = result.rowcount
    
    logger.info(f"   ✅ Transformados {rows} registros para stg.dre")
    return rows


def transform_aliquota(engine: Engine) -> int:
    """
    Transforma raw.aliquota → stg.aliquota
    
    Transformações:
    - Adiciona mes_num e data_key
    - Padroniza tipo de imposto
    """
    print("\n🔄 Transformando alíquotas: RAW → STG")
    
    config = get_config()
    ano = config.get_etl_config().get("ano_referencia", 2025)
    
    query = f"""
        INSERT INTO stg.aliquota (
            tipo_imposto, mes_num, mes_nome, data_key, aliquota,
            raw_created_at, stg_loaded_at
        )
        SELECT
            TRIM(tipo_imposto) as tipo_imposto,
            CASE UPPER(TRIM(mes))
                WHEN 'JAN' THEN 1 WHEN 'FEV' THEN 2 WHEN 'MAR' THEN 3
                WHEN 'ABR' THEN 4 WHEN 'MAI' THEN 5 WHEN 'JUN' THEN 6
                WHEN 'JUL' THEN 7 WHEN 'AGO' THEN 8 WHEN 'SET' THEN 9
                WHEN 'OUT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEZ' THEN 12
            END as mes_num,
            UPPER(TRIM(mes)) as mes_nome,
            '{ano}-' || LPAD(
                CASE UPPER(TRIM(mes))
                    WHEN 'JAN' THEN '1' WHEN 'FEV' THEN '2' WHEN 'MAR' THEN '3'
                    WHEN 'ABR' THEN '4' WHEN 'MAI' THEN '5' WHEN 'JUN' THEN '6'
                    WHEN 'JUL' THEN '7' WHEN 'AGO' THEN '8' WHEN 'SET' THEN '9'
                    WHEN 'OUT' THEN '10' WHEN 'NOV' THEN '11' WHEN 'DEZ' THEN '12'
                END, 2, '0'
            ) as data_key,
            aliquota,
            created_at as raw_created_at,
            NOW() as stg_loaded_at
        FROM raw.aliquota
        WHERE aliquota IS NOT NULL
          AND mes IS NOT NULL
          AND tipo_imposto IS NOT NULL
    """
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE stg.aliquota"))
        result = conn.execute(text(query))
        conn.commit()
        rows = result.rowcount
    
    logger.info(f"   ✅ Transformados {rows} registros para stg.aliquota")
    return rows


# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def run_transform_raw_to_stg(engine: Optional[Engine] = None) -> Dict[str, int]:
    """
    Executa todas as transformações RAW → STG.
    
    Args:
        engine: Engine SQLAlchemy (opcional)
        
    Returns:
        Dicionário com contagem de registros por tabela
    """
    print("\n" + "=" * 60)
    print("   TRANSFORMAÇÃO: RAW → STG (Silver)")
    print("=" * 60)
    
    engine = engine or get_engine()
    
    results = {
        'receita': transform_receita(engine),
        'despesa': transform_despesa(engine),
        'dre': transform_dre(engine),
        'aliquota': transform_aliquota(engine)
    }
    
    total = sum(results.values())
    print(f"\n✅ Transformação RAW→STG completa: {total} registros")
    
    return results


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    run_transform_raw_to_stg()
