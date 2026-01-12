#!/bin/bash
# =============================================================================
# DRE Analytics 2025 - Script de Execução do ETL (Linux/Mac)
# =============================================================================

echo "============================================"
echo " DRE Analytics 2025 - ETL Pipeline"
echo " $(date)"
echo "============================================"

# Navega para o diretório do script
cd "$(dirname "$0")"

# Ativa o ambiente virtual se existir
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

# Executa o pipeline
python -m etl._05_run_pipeline --log-level INFO --triggered-by "Cron"

# Verifica resultado
if [ $? -eq 0 ]; then
    echo ""
    echo "============================================"
    echo " Pipeline executado com sucesso!"
    echo "============================================"
else
    echo ""
    echo "============================================"
    echo " ERRO: Pipeline falhou"
    echo "============================================"
    exit 1
fi
