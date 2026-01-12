"""
DRE Analytics 2025 - API
========================

API REST para ingestão e consulta de dados da DRE.

FLUXO:
1. POST /api/v1/upload    → Recebe Excel, salva, dispara pipeline
2. GET  /api/v1/dre/*     → Consulta dados processados

Para executar:
    uvicorn api.main:app --reload --port 8000

Documentação:
    http://localhost:8000/docs
"""

import os
import sys
import shutil
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import text

# Adiciona o diretório raiz ao path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl._00_config import get_config, get_engine, PROJECT_ROOT


# =============================================================================
# APP
# =============================================================================

app = FastAPI(
    title="DRE Analytics 2025",
    description="Ingestão e consulta de dados da DRE",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# MODELOS
# =============================================================================

class HealthResponse(BaseModel):
    status: str
    database: str
    last_upload: Optional[str]
    timestamp: datetime


class UploadResponse(BaseModel):
    message: str
    filename: str
    pipeline_status: str
    rows_processed: Optional[int]


class DREResumo(BaseModel):
    receita_bruta: float
    receita_liquida: float
    ebitda: float
    lucro_liquido: float
    margem_ebitda: float
    margem_liquida: float


class DREItem(BaseModel):
    mes: str
    linha: str
    valor: float


# =============================================================================
# HELPERS
# =============================================================================

def get_last_upload_info() -> Optional[str]:
    """Retorna info do último arquivo carregado"""
    config = get_config()
    excel_path = Path(PROJECT_ROOT) / config.get('paths', {}).get('source_excel', '')
    if excel_path.exists():
        mtime = datetime.fromtimestamp(excel_path.stat().st_mtime)
        return f"{excel_path.name} ({mtime.strftime('%d/%m/%Y %H:%M')})"
    return None


def run_pipeline_sync() -> dict:
    """Executa pipeline de forma síncrona"""
    from etl._05_run_pipeline import run_pipeline
    return run_pipeline(triggered_by='API Upload')


# =============================================================================
# ENDPOINTS - INGESTÃO
# =============================================================================

@app.get("/", tags=["Root"])
async def root():
    return {
        "api": "DRE Analytics 2025",
        "docs": "/docs",
        "endpoints": {
            "upload": "POST /api/v1/upload",
            "dre": "GET /api/v1/dre",
            "health": "GET /api/v1/health"
        }
    }


@app.get("/api/v1/health", response_model=HealthResponse, tags=["Sistema"])
async def health():
    """Status do sistema"""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return HealthResponse(
        status="healthy" if db_status == "connected" else "degraded",
        database=db_status,
        last_upload=get_last_upload_info(),
        timestamp=datetime.now()
    )


@app.post("/api/v1/upload", response_model=UploadResponse, tags=["Ingestão"])
async def upload_excel(file: UploadFile = File(...)):
    """
    Upload de arquivo Excel para processamento.
    
    Fluxo:
    1. Recebe arquivo .xlsx
    2. Salva em 01_dados_originais/
    3. Executa pipeline ETL completo
    4. Retorna status
    """
    # Validar extensão
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(400, "Apenas arquivos Excel (.xlsx, .xls)")
    
    try:
        # Caminho de destino
        config = get_config()
        dest_path = Path(PROJECT_ROOT) / config.get('paths', {}).get('source_excel', 'data/input.xlsx')
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Salvar arquivo
        with open(dest_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        
        # Executar pipeline
        result = run_pipeline_sync()
        
        return UploadResponse(
            message="Arquivo processado com sucesso",
            filename=file.filename,
            pipeline_status=result.get('status', 'unknown'),
            rows_processed=result.get('total_rows', 0)
        )
        
    except Exception as e:
        raise HTTPException(500, f"Erro no processamento: {str(e)}")


@app.post("/api/v1/pipeline/run", tags=["Ingestão"])
async def trigger_pipeline():
    """Executa pipeline manualmente (sem upload)"""
    try:
        result = run_pipeline_sync()
        return {
            "message": "Pipeline executado",
            "status": result.get('status'),
            "duration_seconds": result.get('duration_seconds')
        }
    except Exception as e:
        raise HTTPException(500, str(e))


# =============================================================================
# ENDPOINTS - CONSULTA
# =============================================================================

@app.get("/api/v1/dre", response_model=DREResumo, tags=["Consulta"])
async def get_dre():
    """Resumo da DRE (totais anuais)"""
    try:
        engine = get_engine()
        
        query = """
            SELECT linha_dre, SUM(valor) as total
            FROM dw.fact_dre
            GROUP BY linha_dre
        """
        
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
        
        values = {row[0].upper(): float(row[1]) for row in rows}
        
        receita = values.get('RECEITA BRUTA', 0)
        ebitda = values.get('EBITDA', 0)
        lucro = values.get('LUCRO LÍQUIDO', 0)
        
        return DREResumo(
            receita_bruta=receita,
            receita_liquida=values.get('RECEITA LÍQUIDA', 0),
            ebitda=ebitda,
            lucro_liquido=lucro,
            margem_ebitda=(ebitda / receita * 100) if receita else 0,
            margem_liquida=(lucro / receita * 100) if receita else 0
        )
        
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/v1/dre/mensal", response_model=List[DREItem], tags=["Consulta"])
async def get_dre_mensal(linha: Optional[str] = None):
    """DRE detalhado por mês"""
    try:
        engine = get_engine()
        
        query = """
            SELECT c.mes_nome, f.linha_dre, f.valor
            FROM dw.fact_dre f
            JOIN dw.dim_calendario c ON f.data_key = c.data_key
        """
        if linha:
            query += f" WHERE UPPER(f.linha_dre) LIKE UPPER('%{linha}%')"
        query += " ORDER BY c.mes_num"
        
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
        
        return [DREItem(mes=r[0], linha=r[1], valor=float(r[2])) for r in rows]
        
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/v1/receita", tags=["Consulta"])
async def get_receita(cenario: Optional[str] = None):
    """Resumo de receitas por tipo"""
    try:
        engine = get_engine()
        
        where = f"WHERE cenario = '{cenario}'" if cenario else ""
        
        query = f"""
            SELECT cenario, tipo_receita, SUM(valor) as total
            FROM dw.fact_receita
            {where}
            GROUP BY cenario, tipo_receita
            ORDER BY cenario, total DESC
        """
        
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
        
        return [{"cenario": r[0], "tipo": r[1], "total": float(r[2])} for r in rows]
        
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/v1/despesa", tags=["Consulta"])
async def get_despesa(cenario: Optional[str] = None, top: int = 10):
    """Top despesas por pacote"""
    try:
        engine = get_engine()
        
        where = f"WHERE cenario = '{cenario}'" if cenario else ""
        
        query = f"""
            SELECT cenario, pacote, SUM(valor) as total
            FROM dw.fact_despesa
            {where}
            GROUP BY cenario, pacote
            ORDER BY ABS(SUM(valor)) DESC
            LIMIT {top}
        """
        
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
        
        return [{"cenario": r[0], "pacote": r[1], "total": float(r[2])} for r in rows]
        
    except Exception as e:
        raise HTTPException(500, str(e))


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
