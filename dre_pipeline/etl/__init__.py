"""
DRE Analytics 2025 - ETL Package
"""

__version__ = "1.0.0"

# Imports dos módulos
from etl._00_config import get_config, get_engine, test_connection
from etl._01_extract_excel import run_extract
from etl._02_transform_raw_to_stg import run_transform_raw_to_stg
from etl._03_transform_stg_to_dw import run_transform_stg_to_dw
from etl._04_dq_checks import run_dq_checks
from etl._05_run_pipeline import run_pipeline

__all__ = [
    'get_config',
    'get_engine', 
    'test_connection',
    'run_extract',
    'run_transform_raw_to_stg',
    'run_transform_stg_to_dw',
    'run_dq_checks',
    'run_pipeline',
]
