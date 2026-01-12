@echo off
REM =============================================================================
REM DRE Analytics 2025 - Setup Inicial do Banco de Dados
REM =============================================================================
REM
REM Este script cria todas as tabelas no PostgreSQL
REM Execute após iniciar os containers com: docker-compose up -d
REM

echo ============================================
echo  DRE Analytics 2025 - Database Setup
echo  %date% %time%
echo ============================================

cd /d "%~dp0"

echo.
echo [1/5] Criando schemas...
docker exec -i dre_postgres psql -U dre_user -d dre_db < sql\01_create_schemas.sql
if %ERRORLEVEL% NEQ 0 goto :error

echo [2/5] Criando tabelas RAW...
docker exec -i dre_postgres psql -U dre_user -d dre_db < sql\02_create_raw_tables.sql
if %ERRORLEVEL% NEQ 0 goto :error

echo [3/5] Criando tabelas STG...
docker exec -i dre_postgres psql -U dre_user -d dre_db < sql\03_create_stg_tables.sql
if %ERRORLEVEL% NEQ 0 goto :error

echo [4/5] Criando tabelas DW (Star Schema)...
docker exec -i dre_postgres psql -U dre_user -d dre_db < sql\04_create_dw_tables.sql
if %ERRORLEVEL% NEQ 0 goto :error

echo [5/5] Criando tabelas de auditoria...
docker exec -i dre_postgres psql -U dre_user -d dre_db < sql\05_create_audit_tables.sql
if %ERRORLEVEL% NEQ 0 goto :error

echo.
echo ============================================
echo  Setup concluido com sucesso!
echo ============================================
echo.
echo Proximos passos:
echo   1. pip install -r requirements.txt
echo   2. run_etl.bat
echo   3. uvicorn api.main:app --reload
echo.
echo Adminer (UI SQL): http://localhost:8080
echo.

pause
goto :eof

:error
echo.
echo ============================================
echo  ERRO: Falha no setup do banco de dados
echo  Verifique se o Docker está rodando
echo  Execute: docker-compose up -d
echo ============================================
echo.
pause
exit /b 1
