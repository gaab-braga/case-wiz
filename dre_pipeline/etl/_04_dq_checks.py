"""
DRE Analytics 2025 - Pipeline ETL
Verificações de Qualidade de Dados (DQ Checks)

Executa validações e registra resultados na tabela dw.data_quality_results.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ._00_config import get_engine, get_config


# =============================================================================
# CONFIGURAÇÃO DE LOGGING
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# DEFINIÇÃO DOS CHECKS
# =============================================================================

def get_dq_checks() -> List[Dict]:
    """
    Retorna lista de verificações de DQ.
    
    Cada check retorna:
    - status: PASS, FAIL, WARN
    - actual_value: valor encontrado
    - expected_value: valor esperado
    - message: descrição do resultado
    """
    config = get_config()
    dq_config = config.get_dq_config()
    
    receita_real = dq_config.get('receita_bruta_realizado', 67629718.14)
    receita_orc = dq_config.get('receita_bruta_orcado', 68369172.32)
    desp_real = dq_config.get('despesas_realizado', -41613267.98)
    desp_orc = dq_config.get('despesas_orcado', -48774529.00)
    lucro = dq_config.get('lucro_liquido', 18572919.69)
    tolerance = dq_config.get('tolerance_percent', 0.01)
    
    return [
        {
            'rule_id': 1,
            'rule_name': 'receita_realizado_total',
            'description': 'Valida total de Receita Bruta Realizado',
            'query': f"""
                WITH totals AS (
                    SELECT SUM(valor) as total
                    FROM dw.fact_receita
                    WHERE cenario = 'Realizado'
                      AND tipo_receita IN ('SALES', 'SERVICE')
                )
                SELECT 
                    CASE 
                        WHEN ABS(total - {receita_real}) / {receita_real} <= {tolerance}
                        THEN 'PASS' 
                        ELSE 'FAIL' 
                    END as status,
                    ROUND(total::numeric, 2) as actual_value,
                    {receita_real} as expected_value,
                    'Receita Realizado: R$ ' || TO_CHAR(total, 'FM999,999,999.00') as message
                FROM totals
            """
        },
        {
            'rule_id': 2,
            'rule_name': 'receita_orcado_total',
            'description': 'Valida total de Receita Bruta Orçado',
            'query': f"""
                WITH totals AS (
                    SELECT SUM(valor) as total
                    FROM dw.fact_receita
                    WHERE cenario = 'Orçado'
                      AND tipo_receita IN ('SALES', 'SERVICE')
                )
                SELECT 
                    CASE 
                        WHEN ABS(total - {receita_orc}) / {receita_orc} <= {tolerance}
                        THEN 'PASS' 
                        ELSE 'FAIL' 
                    END as status,
                    ROUND(total::numeric, 2) as actual_value,
                    {receita_orc} as expected_value,
                    'Receita Orçado: R$ ' || TO_CHAR(total, 'FM999,999,999.00') as message
                FROM totals
            """
        },
        {
            'rule_id': 3,
            'rule_name': 'despesas_realizado_total',
            'description': 'Valida total de Despesas Realizado',
            'query': f"""
                WITH totals AS (
                    SELECT SUM(valor) as total
                    FROM dw.fact_despesa
                    WHERE cenario = 'Realizado'
                )
                SELECT 
                    CASE 
                        WHEN ABS(total - ({desp_real})) / ABS({desp_real}) <= {tolerance}
                        THEN 'PASS' 
                        ELSE 'FAIL' 
                    END as status,
                    ROUND(total::numeric, 2) as actual_value,
                    {desp_real} as expected_value,
                    'Despesas Realizado: R$ ' || TO_CHAR(total, 'FM999,999,999.00') as message
                FROM totals
            """
        },
        {
            'rule_id': 4,
            'rule_name': 'despesas_orcado_total',
            'description': 'Valida total de Despesas Orçado',
            'query': f"""
                WITH totals AS (
                    SELECT SUM(valor) as total
                    FROM dw.fact_despesa
                    WHERE cenario = 'Orçado'
                )
                SELECT 
                    CASE 
                        WHEN ABS(total - ({desp_orc})) / ABS({desp_orc}) <= {tolerance}
                        THEN 'PASS' 
                        ELSE 'FAIL' 
                    END as status,
                    ROUND(total::numeric, 2) as actual_value,
                    {desp_orc} as expected_value,
                    'Despesas Orçado: R$ ' || TO_CHAR(total, 'FM999,999,999.00') as message
                FROM totals
            """
        },
        {
            'rule_id': 5,
            'rule_name': 'lucro_liquido_dre',
            'description': 'Valida Lucro Líquido no modelo DRE',
            'query': f"""
                WITH totals AS (
                    SELECT SUM(valor) as total
                    FROM dw.fact_dre
                    WHERE UPPER(linha_dre) = 'LUCRO LÍQUIDO'
                )
                SELECT 
                    CASE 
                        WHEN ABS(total - {lucro}) / {lucro} <= {tolerance}
                        THEN 'PASS' 
                        ELSE 'FAIL' 
                    END as status,
                    ROUND(total::numeric, 2) as actual_value,
                    {lucro} as expected_value,
                    'Lucro Líquido: R$ ' || TO_CHAR(total, 'FM999,999,999.00') as message
                FROM totals
            """
        },
        {
            'rule_id': 6,
            'rule_name': 'fact_receita_not_empty',
            'description': 'Verifica se fact_receita tem dados',
            'query': """
                SELECT 
                    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as status,
                    COUNT(*) as actual_value,
                    1 as expected_value,
                    'Total de registros: ' || COUNT(*)::text as message
                FROM dw.fact_receita
            """
        },
        {
            'rule_id': 7,
            'rule_name': 'fact_despesa_not_empty',
            'description': 'Verifica se fact_despesa tem dados',
            'query': """
                SELECT 
                    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as status,
                    COUNT(*) as actual_value,
                    1 as expected_value,
                    'Total de registros: ' || COUNT(*)::text as message
                FROM dw.fact_despesa
            """
        },
        {
            'rule_id': 8,
            'rule_name': 'fact_dre_not_empty',
            'description': 'Verifica se fact_dre tem dados',
            'query': """
                SELECT 
                    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as status,
                    COUNT(*) as actual_value,
                    1 as expected_value,
                    'Total de registros: ' || COUNT(*)::text as message
                FROM dw.fact_dre
            """
        },
        {
            'rule_id': 9,
            'rule_name': 'all_months_present_receita',
            'description': 'Verifica se todos os 12 meses estão presentes em receita',
            'query': """
                SELECT 
                    CASE WHEN COUNT(DISTINCT data_key) = 12 THEN 'PASS' ELSE 'FAIL' END as status,
                    COUNT(DISTINCT data_key) as actual_value,
                    12 as expected_value,
                    'Meses únicos: ' || COUNT(DISTINCT data_key)::text as message
                FROM dw.fact_receita
            """
        },
        {
            'rule_id': 10,
            'rule_name': 'data_keys_valid',
            'description': 'Verifica se todas as data_keys referenciam dim_calendario',
            'query': """
                SELECT 
                    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status,
                    COUNT(*) as actual_value,
                    0 as expected_value,
                    CASE 
                        WHEN COUNT(*) = 0 THEN 'Todas as data_keys são válidas'
                        ELSE 'Encontradas ' || COUNT(*)::text || ' data_keys órfãs'
                    END as message
                FROM dw.fact_receita f
                LEFT JOIN dw.dim_calendario d ON f.data_key = d.data_key
                WHERE d.data_key IS NULL
            """
        },
        {
            'rule_id': 11,
            'rule_name': 'margem_liquida_sanity',
            'description': 'Verifica se margem líquida está em range razoável (20-40%)',
            'query': f"""
                WITH metrics AS (
                    SELECT 
                        (SELECT SUM(valor) FROM dw.fact_dre WHERE UPPER(linha_dre) = 'LUCRO LÍQUIDO') as lucro,
                        (SELECT SUM(valor) FROM dw.fact_dre WHERE UPPER(linha_dre) = 'RECEITA BRUTA') as receita
                )
                SELECT 
                    CASE 
                        WHEN (lucro / NULLIF(receita, 0)) BETWEEN 0.20 AND 0.40 
                        THEN 'PASS' 
                        ELSE 'WARN' 
                    END as status,
                    ROUND((lucro / NULLIF(receita, 0) * 100)::numeric, 2) as actual_value,
                    27.5 as expected_value,
                    'Margem Líquida: ' || ROUND((lucro / NULLIF(receita, 0) * 100)::numeric, 2)::text || '%' as message
                FROM metrics
            """
        },
        {
            'rule_id': 12,
            'rule_name': 'service_maior_que_sales',
            'description': 'Verifica se SERVICE > SALES (esperado no negócio)',
            'query': """
                WITH types AS (
                    SELECT 
                        SUM(CASE WHEN tipo_receita = 'SALES' THEN valor ELSE 0 END) as sales,
                        SUM(CASE WHEN tipo_receita = 'SERVICE' THEN valor ELSE 0 END) as service
                    FROM dw.fact_receita
                    WHERE cenario = 'Realizado'
                )
                SELECT 
                    CASE WHEN service > sales THEN 'PASS' ELSE 'WARN' END as status,
                    ROUND((service / NULLIF(sales + service, 0) * 100)::numeric, 1) as actual_value,
                    77 as expected_value,
                    'SERVICE representa ' || ROUND((service / NULLIF(sales + service, 0) * 100)::numeric, 1)::text || '% da receita' as message
                FROM types
            """
        }
    ]


# =============================================================================
# FUNÇÃO DE EXECUÇÃO
# =============================================================================

def run_dq_checks(
    engine: Optional[Engine] = None,
    run_id: Optional[int] = None,
    fail_on_error: bool = False
) -> Dict[str, any]:
    """
    Executa todas as verificações de qualidade de dados.
    
    Args:
        engine: Engine SQLAlchemy (opcional)
        run_id: ID da execução do pipeline (para relacionar resultados)
        fail_on_error: Se True, levanta exceção em caso de FAIL
        
    Returns:
        Dicionário com estatísticas e resultados
    """
    print("\n" + "=" * 60)
    print("   VERIFICAÇÕES DE QUALIDADE DE DADOS")
    print("=" * 60)
    
    engine = engine or get_engine()
    checks = get_dq_checks()
    
    results = []
    passed = 0
    failed = 0
    warned = 0
    
    for check in checks:
        try:
            with engine.connect() as conn:
                result = conn.execute(text(check['query']))
                row = result.fetchone()
                
                if row:
                    status = row[0]
                    actual = row[1]
                    expected = row[2]
                    message = row[3]
                    
                    # Calcular diferença
                    try:
                        diff = float(actual) - float(expected) if actual and expected else 0
                        diff_pct = (diff / float(expected) * 100) if expected and expected != 0 else 0
                    except:
                        diff = 0
                        diff_pct = 0
                    
                    result_record = {
                        'run_id': run_id,
                        'rule_id': check['rule_id'],
                        'rule_name': check['rule_name'],
                        'rule_description': check['description'],
                        'status': status,
                        'expected_value': expected,
                        'actual_value': actual,
                        'difference_value': diff,
                        'difference_percent': diff_pct,
                        'message': message,
                        'checked_at': datetime.now()
                    }
                    
                    results.append(result_record)
                    
                    # Contagem
                    if status == 'PASS':
                        passed += 1
                        emoji = '✅'
                    elif status == 'FAIL':
                        failed += 1
                        emoji = '❌'
                    else:
                        warned += 1
                        emoji = '⚠️'
                    
                    print(f"   {emoji} [{check['rule_id']:02d}] {check['rule_name']}: {message}")
                    
                    # Inserir resultado no banco
                    if run_id:
                        conn.execute(text("""
                            INSERT INTO dw.data_quality_results (
                                run_id, rule_id, rule_name, rule_description,
                                status, expected_value, actual_value,
                                difference_value, difference_percent, message
                            ) VALUES (
                                :run_id, :rule_id, :rule_name, :rule_description,
                                :status, :expected_value, :actual_value,
                                :difference_value, :difference_percent, :message
                            )
                        """), result_record)
                        conn.commit()
                        
        except Exception as e:
            logger.error(f"   ❌ Erro no check {check['rule_name']}: {e}")
            failed += 1
    
    # Resumo
    print(f"\n📊 Resumo DQ: {passed} PASS | {warned} WARN | {failed} FAIL")
    
    if fail_on_error and failed > 0:
        raise Exception(f"DQ Check falhou: {failed} verificações falharam")
    
    return {
        'total': len(checks),
        'passed': passed,
        'warned': warned,
        'failed': failed,
        'results': results
    }


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    run_dq_checks()
