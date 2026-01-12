"""
DRE Analytics 2025 - Pipeline ETL
Orquestrador do Pipeline

Executa o pipeline completo com controle de execução,
logging e tratamento de erros.
"""

import argparse
import logging
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ._00_config import get_config, get_engine, test_connection
from ._01_extract_excel import run_extract
from ._02_transform_raw_to_stg import run_transform_raw_to_stg
from ._03_transform_stg_to_dw import run_transform_stg_to_dw
from ._04_dq_checks import run_dq_checks


# =============================================================================
# CONFIGURAÇÃO DE LOGGING
# =============================================================================

def setup_logging(log_level: str = 'INFO', log_file: Optional[str] = None):
    """
    Configura o sistema de logging.
    """
    config = get_config()
    logging_config = config.get_logging_config()
    
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Criar pasta de logs se não existir
    log_folder = config.project_root / logging_config.get('folder', 'logs')
    log_folder.mkdir(exist_ok=True)
    
    # Nome do arquivo de log
    if not log_file:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_folder / f'etl_run_{timestamp}.log'
    
    # Configurar handlers
    handlers = [logging.StreamHandler(sys.stdout)]
    
    try:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        handlers.append(file_handler)
    except Exception as e:
        print(f"⚠️ Não foi possível criar arquivo de log: {e}")
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    
    return logging.getLogger(__name__)


# =============================================================================
# CONTROLE DE EXECUÇÃO
# =============================================================================

class ETLRun:
    """
    Gerencia uma execução do pipeline ETL.
    """
    
    def __init__(self, engine: Engine, pipeline_name: str = 'dre_pipeline'):
        self.engine = engine
        self.pipeline_name = pipeline_name
        self.run_id = None
        self.started_at = None
        self.steps = []
    
    def start(self, triggered_by: str = 'MANUAL') -> int:
        """
        Inicia uma nova execução.
        """
        self.started_at = datetime.now()
        
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                INSERT INTO dw.etl_run (started_at, status, triggered_by, pipeline_name)
                VALUES (:started_at, 'RUNNING', :triggered_by, :pipeline_name)
                RETURNING run_id
            """), {
                'started_at': self.started_at,
                'triggered_by': triggered_by,
                'pipeline_name': self.pipeline_name
            })
            conn.commit()
            self.run_id = result.fetchone()[0]
        
        return self.run_id
    
    def log_step(
        self, 
        step_name: str, 
        step_order: int,
        status: str, 
        rows_read: int = 0,
        rows_written: int = 0,
        error_message: str = None
    ):
        """
        Registra um step do pipeline.
        """
        step_info = {
            'run_id': self.run_id,
            'step_name': step_name,
            'step_order': step_order,
            'status': status,
            'rows_read': rows_read,
            'rows_written': rows_written,
            'started_at': datetime.now(),
            'finished_at': datetime.now(),
            'error_message': error_message
        }
        
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO dw.etl_step_log (
                    run_id, step_name, step_order, status,
                    rows_read, rows_written,
                    started_at, finished_at, error_message
                ) VALUES (
                    :run_id, :step_name, :step_order, :status,
                    :rows_read, :rows_written,
                    :started_at, :finished_at, :error_message
                )
            """), step_info)
            conn.commit()
        
        self.steps.append(step_info)
    
    def finish(self, status: str, error_message: str = None):
        """
        Finaliza a execução.
        """
        ended_at = datetime.now()
        
        # Calcular totais
        total_rows = sum(s.get('rows_written', 0) for s in self.steps)
        duration = (ended_at - self.started_at).total_seconds()
        
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE dw.etl_run
                SET status = :status,
                    finished_at = :finished_at,
                    duration_seconds = :duration,
                    total_rows_processed = :total_rows,
                    error_message = :error_message
                WHERE run_id = :run_id
            """), {
                'run_id': self.run_id,
                'status': status,
                'finished_at': ended_at,
                'duration': duration,
                'total_rows': total_rows,
                'error_message': error_message
            })
            conn.commit()


# =============================================================================
# PIPELINE
# =============================================================================

def run_pipeline(
    skip_extract: bool = False,
    skip_dq: bool = False,
    fail_on_dq_error: bool = False,
    log_level: str = 'INFO',
    triggered_by: str = 'MANUAL',
    engine: Optional[Engine] = None
) -> Dict:
    """
    Executa o pipeline ETL completo.
    
    Args:
        skip_extract: Pular extração do Excel
        skip_dq: Pular verificações de DQ
        fail_on_dq_error: Falhar se DQ encontrar erros
        log_level: Nível de log (DEBUG, INFO, WARNING, ERROR)
        triggered_by: Origem da execução (MANUAL, SCHEDULED, API)
        engine: Engine SQLAlchemy (opcional)
        
    Returns:
        Dicionário com estatísticas da execução
    """
    print("\n" + "=" * 70)
    print("   DRE ANALYTICS 2025 - PIPELINE ETL")
    print("=" * 70)
    print(f"   Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Disparado por: {triggered_by}")
    print("=" * 70)
    
    # Setup
    logger = setup_logging(log_level)
    config = get_config()
    engine = engine or get_engine()
    
    # Verificar conexão
    print("\n🔌 Verificando conexão com banco de dados...")
    if not test_connection():
        print("❌ Falha na conexão com o banco. Verifique se o Docker está rodando.")
        return {'status': 'FAILED', 'error': 'Database connection failed'}
    print("   ✅ Conexão OK")
    
    # Iniciar execução
    etl_run = ETLRun(engine)
    run_id = etl_run.start(triggered_by)
    print(f"\n🚀 Execução iniciada - Run ID: {run_id}")
    
    try:
        step_order = 0
        
        # =====================================================================
        # STEP 1: Extração Excel → RAW
        # =====================================================================
        if not skip_extract:
            step_order += 1
            print(f"\n{'─' * 60}")
            print(f"   STEP {step_order}: Extração Excel → RAW")
            print(f"{'─' * 60}")
            
            try:
                extract_results = run_extract(engine=engine)
                total_extracted = sum(r.get('rows_loaded', 0) for r in extract_results.values())
                etl_run.log_step('extract_excel', step_order, 'SUCCESS', rows_written=total_extracted)
            except Exception as e:
                etl_run.log_step('extract_excel', step_order, 'FAILED', error_message=str(e))
                raise
        else:
            print("\n⏭️ Extração ignorada (--skip-extract)")
        
        # =====================================================================
        # STEP 2: Transformação RAW → STG
        # =====================================================================
        step_order += 1
        print(f"\n{'─' * 60}")
        print(f"   STEP {step_order}: Transformação RAW → STG")
        print(f"{'─' * 60}")
        
        try:
            stg_results = run_transform_raw_to_stg(engine=engine)
            total_stg = sum(stg_results.values())
            etl_run.log_step('transform_raw_to_stg', step_order, 'SUCCESS', rows_written=total_stg)
        except Exception as e:
            etl_run.log_step('transform_raw_to_stg', step_order, 'FAILED', error_message=str(e))
            raise
        
        # =====================================================================
        # STEP 3: Transformação STG → DW
        # =====================================================================
        step_order += 1
        print(f"\n{'─' * 60}")
        print(f"   STEP {step_order}: Transformação STG → DW")
        print(f"{'─' * 60}")
        
        try:
            dw_results = run_transform_stg_to_dw(engine=engine)
            total_dw = sum(dw_results.values())
            etl_run.log_step('transform_stg_to_dw', step_order, 'SUCCESS', rows_written=total_dw)
        except Exception as e:
            etl_run.log_step('transform_stg_to_dw', step_order, 'FAILED', error_message=str(e))
            raise
        
        # =====================================================================
        # STEP 4: Verificações de DQ
        # =====================================================================
        if not skip_dq:
            step_order += 1
            print(f"\n{'─' * 60}")
            print(f"   STEP {step_order}: Verificações de Qualidade")
            print(f"{'─' * 60}")
            
            try:
                dq_results = run_dq_checks(
                    engine=engine, 
                    run_id=run_id,
                    fail_on_error=fail_on_dq_error
                )
                
                status = 'SUCCESS' if dq_results['failed'] == 0 else 'WARNING'
                etl_run.log_step('dq_checks', step_order, status)
            except Exception as e:
                etl_run.log_step('dq_checks', step_order, 'FAILED', error_message=str(e))
                if fail_on_dq_error:
                    raise
        else:
            print("\n⏭️ Verificações DQ ignoradas (--skip-dq)")
        
        # =====================================================================
        # FINALIZAÇÃO
        # =====================================================================
        etl_run.finish('SUCCESS')
        
        duration = (datetime.now() - etl_run.started_at).total_seconds()
        
        print("\n" + "=" * 70)
        print("   ✅ PIPELINE CONCLUÍDO COM SUCESSO!")
        print("=" * 70)
        print(f"   Run ID: {run_id}")
        print(f"   Duração: {duration:.2f} segundos")
        print(f"   Total de steps: {step_order}")
        print("=" * 70)
        
        return {
            'status': 'SUCCESS',
            'run_id': run_id,
            'duration_seconds': duration,
            'steps': step_order
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Pipeline falhou: {error_msg}")
        logger.error(traceback.format_exc())
        
        etl_run.finish('FAILED', error_message=error_msg)
        
        print("\n" + "=" * 70)
        print("   ❌ PIPELINE FALHOU!")
        print("=" * 70)
        print(f"   Erro: {error_msg}")
        print("=" * 70)
        
        return {
            'status': 'FAILED',
            'run_id': run_id,
            'error': error_msg
        }


# =============================================================================
# CLI
# =============================================================================

def main():
    """
    Entry point para CLI.
    """
    parser = argparse.ArgumentParser(
        description='DRE Analytics 2025 - Pipeline ETL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python -m etl._05_run_pipeline                    # Pipeline completo
  python -m etl._05_run_pipeline --skip-extract     # Sem extração
  python -m etl._05_run_pipeline --skip-dq          # Sem validações
  python -m etl._05_run_pipeline --log-level DEBUG  # Modo debug
        """
    )
    
    parser.add_argument(
        '--skip-extract',
        action='store_true',
        help='Pular extração do Excel (usar dados já carregados)'
    )
    
    parser.add_argument(
        '--skip-dq',
        action='store_true',
        help='Pular verificações de qualidade de dados'
    )
    
    parser.add_argument(
        '--fail-on-dq-error',
        action='store_true',
        help='Falhar pipeline se verificações DQ encontrarem erros'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Nível de log (default: INFO)'
    )
    
    parser.add_argument(
        '--triggered-by',
        choices=['MANUAL', 'SCHEDULED', 'API'],
        default='MANUAL',
        help='Origem da execução (default: MANUAL)'
    )
    
    args = parser.parse_args()
    
    result = run_pipeline(
        skip_extract=args.skip_extract,
        skip_dq=args.skip_dq,
        fail_on_dq_error=args.fail_on_dq_error,
        log_level=args.log_level,
        triggered_by=args.triggered_by
    )
    
    sys.exit(0 if result['status'] == 'SUCCESS' else 1)


if __name__ == "__main__":
    main()
