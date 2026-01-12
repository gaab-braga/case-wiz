"""
DRE Analytics 2025 - Pipeline ETL
Extração de Dados do Excel

Lê o arquivo dados_case_pbi.xlsx e carrega nas tabelas RAW.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ._00_config import (
    get_config, get_engine, generate_batch_id,
    MESES_MAP, get_data_key
)


# =============================================================================
# CONFIGURAÇÃO DE LOGGING
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# FUNÇÕES DE EXTRAÇÃO - RECEITA
# =============================================================================

def extract_receita_realizado(
    file_path: Path,
    sheet_name: str,
    engine: Engine,
    batch_id: str
) -> Dict[str, int]:
    """
    Extrai dados de Receita Bruta Realizado para RAW.
    
    O arquivo tem estrutura de relatório com:
    - Linha 0: header com meses
    - Linhas seguintes: SALES/SERVICE > Unidades > Valores
    """
    print(f"\n📥 Extraindo Receita Realizado: {sheet_name}")
    
    config = get_config()
    ano = config.get_etl_config().get("ano_referencia", 2025)
    
    # Ler Excel sem header para processar manualmente
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    
    # Identificar colunas de meses (linha 0)
    header_row = df_raw.iloc[0].tolist()
    mes_cols = {}
    for idx, val in enumerate(header_row):
        if pd.notna(val):
            val_str = str(val).strip().upper()
            if val_str in MESES_MAP:
                mes_cols[idx] = val_str
    
    # Processar dados
    records = []
    current_type = None  # SALES ou SERVICE
    
    for row_idx in range(1, len(df_raw)):
        row = df_raw.iloc[row_idx]
        first_col = row.iloc[0] if pd.notna(row.iloc[0]) else ""
        first_col_str = str(first_col).strip().upper()
        
        # Detectar tipo de receita
        if first_col_str == "SALES":
            current_type = "SALES"
            continue
        elif first_col_str == "SERVICE":
            current_type = "SERVICE"
            continue
        elif first_col_str in ["TOTAL", "CONSOLIDADO", ""]:
            continue
        
        # Se temos um tipo definido e a primeira coluna tem texto, é uma unidade
        if current_type and first_col_str and first_col_str not in ["TOTAL", "CONSOLIDADO"]:
            unidade = str(first_col).strip()
            
            # Extrair valores de cada mês
            for col_idx, mes in mes_cols.items():
                valor = row.iloc[col_idx]
                if pd.notna(valor) and isinstance(valor, (int, float)):
                    records.append({
                        'cenario': 'Realizado',
                        'tipo_receita': current_type,
                        'unidade': unidade,
                        'mes': mes,
                        'valor': float(valor),
                        'source_file': str(file_path.name),
                        'source_sheet': sheet_name,
                        'source_row': row_idx + 1,
                        'batch_id': batch_id
                    })
    
    # Carregar no banco
    if records:
        df_insert = pd.DataFrame(records)
        df_insert.to_sql('receita', engine, schema='raw', if_exists='append', index=False)
        logger.info(f"   ✅ Inseridos {len(records)} registros em raw.receita")
    
    return {'rows_loaded': len(records), 'status': 'success'}


def extract_receita_orcado(
    file_path: Path,
    sheet_name: str,
    engine: Engine,
    batch_id: str
) -> Dict[str, int]:
    """
    Extrai dados de Receita Bruta Orçamento para RAW.
    Mesma estrutura do Realizado.
    """
    print(f"\n📥 Extraindo Receita Orçado: {sheet_name}")
    
    config = get_config()
    ano = config.get_etl_config().get("ano_referencia", 2025)
    
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    
    # Identificar colunas de meses
    header_row = df_raw.iloc[0].tolist()
    mes_cols = {}
    for idx, val in enumerate(header_row):
        if pd.notna(val):
            val_str = str(val).strip().upper()
            if val_str in MESES_MAP:
                mes_cols[idx] = val_str
    
    # Processar dados
    records = []
    current_type = None
    
    for row_idx in range(1, len(df_raw)):
        row = df_raw.iloc[row_idx]
        first_col = row.iloc[0] if pd.notna(row.iloc[0]) else ""
        first_col_str = str(first_col).strip().upper()
        
        if first_col_str == "SALES":
            current_type = "SALES"
            continue
        elif first_col_str == "SERVICE":
            current_type = "SERVICE"
            continue
        elif first_col_str in ["TOTAL", "CONSOLIDADO", ""]:
            continue
        
        if current_type and first_col_str and first_col_str not in ["TOTAL", "CONSOLIDADO"]:
            unidade = str(first_col).strip()
            
            for col_idx, mes in mes_cols.items():
                valor = row.iloc[col_idx]
                if pd.notna(valor) and isinstance(valor, (int, float)):
                    records.append({
                        'cenario': 'Orçado',
                        'tipo_receita': current_type,
                        'unidade': unidade,
                        'mes': mes,
                        'valor': float(valor),
                        'source_file': str(file_path.name),
                        'source_sheet': sheet_name,
                        'source_row': row_idx + 1,
                        'batch_id': batch_id
                    })
    
    if records:
        df_insert = pd.DataFrame(records)
        df_insert.to_sql('receita', engine, schema='raw', if_exists='append', index=False)
        logger.info(f"   ✅ Inseridos {len(records)} registros em raw.receita")
    
    return {'rows_loaded': len(records), 'status': 'success'}


# =============================================================================
# FUNÇÕES DE EXTRAÇÃO - DESPESAS
# =============================================================================

def extract_despesas_realizado(
    file_path: Path,
    sheet_name: str,
    engine: Engine,
    batch_id: str
) -> Dict[str, int]:
    """
    Extrai dados de Despesas Realizado para RAW.
    
    Formato tabular com colunas:
    - Data, Unidade, Pacote, Conta, Valor
    """
    print(f"\n📥 Extraindo Despesas Realizado: {sheet_name}")
    
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # Normalizar nomes de colunas
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # Mapear colunas esperadas
    column_mapping = {
        'data': ['data', 'date', 'dt'],
        'unidade': ['unidade', 'unit', 'centro'],
        'pacote': ['pacote', 'package', 'grupo'],
        'conta': ['conta', 'account', 'descrição', 'descricao'],
        'valor': ['valor', 'value', 'amount', 'vlr']
    }
    
    # Encontrar colunas reais
    real_cols = {}
    for target, options in column_mapping.items():
        for opt in options:
            if opt in df.columns:
                real_cols[target] = opt
                break
    
    # Preparar registros
    records = []
    for idx, row in df.iterrows():
        try:
            data_val = row.get(real_cols.get('data', 'data'))
            if pd.isna(data_val):
                continue
                
            # Converter data
            if isinstance(data_val, str):
                data = pd.to_datetime(data_val).date()
            else:
                data = pd.to_datetime(data_val).date()
            
            records.append({
                'cenario': 'Realizado',
                'data': data,
                'unidade': str(row.get(real_cols.get('unidade', 'unidade'), '')).strip(),
                'pacote': str(row.get(real_cols.get('pacote', 'pacote'), '')).strip(),
                'conta': str(row.get(real_cols.get('conta', 'conta'), '')).strip(),
                'valor': float(row.get(real_cols.get('valor', 'valor'), 0)),
                'source_file': str(file_path.name),
                'source_sheet': sheet_name,
                'source_row': idx + 2,
                'batch_id': batch_id
            })
        except Exception as e:
            logger.warning(f"   ⚠️ Erro na linha {idx+2}: {e}")
            continue
    
    if records:
        df_insert = pd.DataFrame(records)
        df_insert.to_sql('despesa', engine, schema='raw', if_exists='append', index=False)
        logger.info(f"   ✅ Inseridos {len(records)} registros em raw.despesa")
    
    return {'rows_loaded': len(records), 'status': 'success'}


def extract_despesas_orcado(
    file_path: Path,
    sheet_name: str,
    engine: Engine,
    batch_id: str
) -> Dict[str, int]:
    """
    Extrai dados de Despesas Orçamento para RAW.
    """
    print(f"\n📥 Extraindo Despesas Orçado: {sheet_name}")
    
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    column_mapping = {
        'data': ['data', 'date', 'dt'],
        'unidade': ['unidade', 'unit', 'centro'],
        'pacote': ['pacote', 'package', 'grupo'],
        'conta': ['conta', 'account', 'descrição', 'descricao'],
        'valor': ['valor', 'value', 'amount', 'vlr']
    }
    
    real_cols = {}
    for target, options in column_mapping.items():
        for opt in options:
            if opt in df.columns:
                real_cols[target] = opt
                break
    
    records = []
    for idx, row in df.iterrows():
        try:
            data_val = row.get(real_cols.get('data', 'data'))
            if pd.isna(data_val):
                continue
                
            if isinstance(data_val, str):
                data = pd.to_datetime(data_val).date()
            else:
                data = pd.to_datetime(data_val).date()
            
            records.append({
                'cenario': 'Orçado',
                'data': data,
                'unidade': str(row.get(real_cols.get('unidade', 'unidade'), '')).strip(),
                'pacote': str(row.get(real_cols.get('pacote', 'pacote'), '')).strip(),
                'conta': str(row.get(real_cols.get('conta', 'conta'), '')).strip(),
                'valor': float(row.get(real_cols.get('valor', 'valor'), 0)),
                'source_file': str(file_path.name),
                'source_sheet': sheet_name,
                'source_row': idx + 2,
                'batch_id': batch_id
            })
        except Exception as e:
            logger.warning(f"   ⚠️ Erro na linha {idx+2}: {e}")
            continue
    
    if records:
        df_insert = pd.DataFrame(records)
        df_insert.to_sql('despesa', engine, schema='raw', if_exists='append', index=False)
        logger.info(f"   ✅ Inseridos {len(records)} registros em raw.despesa")
    
    return {'rows_loaded': len(records), 'status': 'success'}


# =============================================================================
# FUNÇÕES DE EXTRAÇÃO - DRE E ALÍQUOTAS
# =============================================================================

def extract_modelo_dre(
    file_path: Path,
    sheet_name: str,
    engine: Engine,
    batch_id: str
) -> Dict[str, int]:
    """
    Extrai dados do Modelo DRE para RAW.
    
    Estrutura de relatório com linhas DRE × meses.
    """
    print(f"\n📥 Extraindo Modelo DRE: {sheet_name}")
    
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    
    # Identificar colunas de meses (geralmente linha 1 ou 2)
    mes_cols = {}
    for header_row_idx in range(3):
        header_row = df_raw.iloc[header_row_idx].tolist()
        for idx, val in enumerate(header_row):
            if pd.notna(val):
                val_str = str(val).strip().upper()
                if val_str in MESES_MAP and idx not in mes_cols:
                    mes_cols[idx] = val_str
        if mes_cols:
            break
    
    # Definir ordem e categoria das linhas DRE
    dre_lines_config = [
        ('Receita Bruta', 'Receita', 1),
        ('Imposto Sobre Faturamento', 'Imposto', 2),
        ('Comissões de Venda', 'Dedução', 3),
        ('Receita Líquida', 'Receita', 4),
        ('Custos', 'Custo', 5),
        ('EBITDA META', 'Resultado', 6),
        ('PLR', 'Custo', 7),
        ('EBITDA', 'Resultado', 8),
        ('Resultado Não Operacional', 'Resultado', 9),
        ('Resultado Financeiro', 'Resultado', 10),
        ('EBT', 'Resultado', 11),
        ('IR & CSLL', 'Imposto', 12),
        ('Lucro Líquido', 'Resultado', 13),
    ]
    
    dre_line_map = {name.upper(): (cat, ordem) for name, cat, ordem in dre_lines_config}
    
    # Processar dados
    records = []
    
    for row_idx in range(len(df_raw)):
        row = df_raw.iloc[row_idx]
        first_col = row.iloc[0] if pd.notna(row.iloc[0]) else ""
        first_col_str = str(first_col).strip()
        first_col_upper = first_col_str.upper()
        
        # Verificar se é uma linha DRE conhecida
        if first_col_upper in dre_line_map:
            categoria, ordem = dre_line_map[first_col_upper]
            
            for col_idx, mes in mes_cols.items():
                valor = row.iloc[col_idx]
                if pd.notna(valor) and isinstance(valor, (int, float)):
                    records.append({
                        'linha_dre': first_col_str,
                        'categoria': categoria,
                        'mes': mes,
                        'valor': float(valor),
                        'ordem': ordem,
                        'source_file': str(file_path.name),
                        'source_sheet': sheet_name,
                        'source_row': row_idx + 1,
                        'batch_id': batch_id
                    })
    
    if records:
        df_insert = pd.DataFrame(records)
        df_insert.to_sql('dre', engine, schema='raw', if_exists='append', index=False)
        logger.info(f"   ✅ Inseridos {len(records)} registros em raw.dre")
    
    return {'rows_loaded': len(records), 'status': 'success'}


def extract_aliquotas(
    file_path: Path,
    sheet_name: str,
    engine: Engine,
    batch_id: str
) -> Dict[str, int]:
    """
    Extrai dados de Alíquotas de Imposto para RAW.
    
    Estrutura com tipos de imposto × meses.
    """
    print(f"\n📥 Extraindo Alíquotas: {sheet_name}")
    
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    
    # Identificar colunas de meses
    mes_cols = {}
    for header_row_idx in range(3):
        header_row = df_raw.iloc[header_row_idx].tolist()
        for idx, val in enumerate(header_row):
            if pd.notna(val):
                val_str = str(val).strip().upper()
                if val_str in MESES_MAP and idx not in mes_cols:
                    mes_cols[idx] = val_str
        if mes_cols:
            break
    
    # Tipos de imposto conhecidos
    known_types = ['IMPOSTO SOBRE FATURAMENTO', 'IR & CSLL', 'IR E CSLL']
    
    records = []
    
    for row_idx in range(len(df_raw)):
        row = df_raw.iloc[row_idx]
        first_col = row.iloc[0] if pd.notna(row.iloc[0]) else ""
        first_col_str = str(first_col).strip()
        first_col_upper = first_col_str.upper()
        
        # Verificar se é um tipo de imposto
        is_tax_type = any(kt in first_col_upper for kt in known_types)
        
        if is_tax_type:
            for col_idx, mes in mes_cols.items():
                valor = row.iloc[col_idx]
                if pd.notna(valor) and isinstance(valor, (int, float)):
                    records.append({
                        'tipo_imposto': first_col_str,
                        'mes': mes,
                        'aliquota': float(valor),
                        'source_file': str(file_path.name),
                        'source_sheet': sheet_name,
                        'source_row': row_idx + 1,
                        'batch_id': batch_id
                    })
    
    if records:
        df_insert = pd.DataFrame(records)
        df_insert.to_sql('aliquota', engine, schema='raw', if_exists='append', index=False)
        logger.info(f"   ✅ Inseridos {len(records)} registros em raw.aliquota")
    
    return {'rows_loaded': len(records), 'status': 'success'}


# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def run_extract(
    engine: Optional[Engine] = None,
    truncate_before: bool = True
) -> Dict[str, Dict]:
    """
    Executa extração completa do arquivo Excel para camada RAW.
    
    Args:
        engine: Engine SQLAlchemy (opcional)
        truncate_before: Se True, limpa tabelas RAW antes de carregar
        
    Returns:
        Dicionário com estatísticas por extração
    """
    print("\n" + "=" * 60)
    print("   EXTRAÇÃO: Excel → RAW")
    print("=" * 60)
    
    config = get_config()
    engine = engine or get_engine()
    batch_id = generate_batch_id()
    
    # Obter arquivo fonte
    file_path = config.get_source_file_path()
    if not file_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    print(f"📄 Arquivo: {file_path}")
    print(f"🔖 Batch ID: {batch_id}")
    
    # Obter nomes das abas
    sheets = config.get_etl_config().get("sheets", {})
    
    # Truncar tabelas se solicitado
    if truncate_before:
        print("\n🧹 Limpando tabelas RAW...")
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE raw.receita CASCADE"))
            conn.execute(text("TRUNCATE TABLE raw.despesa CASCADE"))
            conn.execute(text("TRUNCATE TABLE raw.dre CASCADE"))
            conn.execute(text("TRUNCATE TABLE raw.aliquota CASCADE"))
            conn.commit()
        print("   ✅ Tabelas RAW limpas")
    
    # Executar extrações
    results = {}
    
    # Receita Realizado
    if sheets.get("receita_realizado"):
        results['receita_realizado'] = extract_receita_realizado(
            file_path, sheets["receita_realizado"], engine, batch_id
        )
    
    # Receita Orçado
    if sheets.get("receita_orcado"):
        results['receita_orcado'] = extract_receita_orcado(
            file_path, sheets["receita_orcado"], engine, batch_id
        )
    
    # Despesas Realizado
    if sheets.get("despesas_realizado"):
        results['despesas_realizado'] = extract_despesas_realizado(
            file_path, sheets["despesas_realizado"], engine, batch_id
        )
    
    # Despesas Orçado
    if sheets.get("despesas_orcado"):
        results['despesas_orcado'] = extract_despesas_orcado(
            file_path, sheets["despesas_orcado"], engine, batch_id
        )
    
    # Modelo DRE
    if sheets.get("modelo_dre"):
        results['modelo_dre'] = extract_modelo_dre(
            file_path, sheets["modelo_dre"], engine, batch_id
        )
    
    # Alíquotas
    if sheets.get("aliquotas"):
        results['aliquotas'] = extract_aliquotas(
            file_path, sheets["aliquotas"], engine, batch_id
        )
    
    # Resumo
    total_rows = sum(r.get('rows_loaded', 0) for r in results.values())
    print(f"\n✅ Extração completa: {total_rows} registros carregados")
    
    return results


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    run_extract()
