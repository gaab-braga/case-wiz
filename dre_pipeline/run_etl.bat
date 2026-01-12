@echo off
REM =============================================================================
REM DRE Analytics 2025 - Script de Execução do ETL
REM =============================================================================
REM
REM Este script pode ser agendado no Windows Task Scheduler
REM Caminho: C:\Users\gafeb\OneDrive\Desktop\Dashboard_DRE_2025\dre_pipeline\run_etl.bat
REM

echo ============================================
echo  DRE Analytics 2025 - ETL Pipeline
echo  %date% %time%
echo ============================================

REM Navega para o diretório do projeto
cd /d "%~dp0"

REM Ativa o ambiente virtual se existir
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Executa o pipeline
python -m etl._05_run_pipeline --log-level INFO --triggered-by "Task Scheduler"

REM Verifica resultado
if %ERRORLEVEL% EQU 0 (
    echo.
    echo ============================================
    echo  Pipeline executado com sucesso!
    echo ============================================
) else (
    echo.
    echo ============================================
    echo  ERRO: Pipeline falhou com codigo %ERRORLEVEL%
    echo ============================================
)

REM Pausa apenas se executado manualmente (não pelo scheduler)
if "%1"=="" (
    echo.
    pause
)
