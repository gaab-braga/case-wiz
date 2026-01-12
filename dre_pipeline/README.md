# DRE Analytics 2025 - Pipeline ETL

Pipeline de dados para análise de Demonstração do Resultado do Exercício (DRE), com arquitetura Medallion (Bronze → Silver → Gold) e API REST.

## 📋 Visão Geral

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DRE Analytics 2025 Pipeline                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐                                                           │
│  │   Upload     │  POST /api/v1/upload (Excel)                              │
│  │   via API    │                                                           │
│  └──────┬───────┘                                                           │
│         │                                                                   │
│         ▼                                                                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │    Bronze    │    │    Silver    │    │     Gold     │                   │
│  │    (RAW)     │ ──▶│    (STG)     │ ──▶│     (DW)     │                   │
│  │  4 tabelas   │    │  4 tabelas   │    │ Star Schema  │                   │
│  └──────────────┘    └──────────────┘    └──────┬───────┘                   │
│                                                 │                           │
│                            ┌────────────────────┼────────────────────┐      │
│                            ▼                    ▼                    ▼      │
│                     ┌──────────────┐    ┌──────────────┐    ┌────────────┐  │
│                     │   API REST   │    │   Power BI   │    │   Adminer  │  │
│                     │  GET /dre/*  │    │ DirectQuery  │    │  (SQL UI)  │  │
│                     └──────────────┘    └──────────────┘    └────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 🔄 Fluxo de Dados

1. **Upload via API** → `POST /api/v1/upload` recebe Excel
2. **Pipeline ETL** → Bronze → Silver → Gold (automático)
3. **Consumo** → API REST ou Power BI conectado ao PostgreSQL

## 🏗️ Estrutura do Projeto

```
dre_pipeline/
├── api/                    # FastAPI REST
│   ├── __init__.py
│   └── main.py            # Endpoints
├── etl/                    # Pipeline ETL
│   ├── __init__.py
│   ├── _00_config.py      # Configuração
│   ├── _01_extract_excel.py  # Bronze Layer
│   ├── _02_transform_raw_to_stg.py  # Silver Layer
│   ├── _03_transform_stg_to_dw.py   # Gold Layer
│   ├── _04_dq_checks.py   # Data Quality
│   └── _05_run_pipeline.py  # Orquestrador
├── sql/                    # DDL Scripts
│   ├── 01_create_schemas.sql
│   ├── 02_create_raw_tables.sql
│   ├── 03_create_stg_tables.sql
│   ├── 04_create_dw_tables.sql
│   └── 05_create_audit_tables.sql
├── data/                   # Dados processados
├── logs/                   # Logs de execução
├── docs/                   # Documentação
├── powerbi/               # Arquivos Power BI
├── tests/                 # Testes
├── config.yml             # Configuração
├── docker-compose.yml     # PostgreSQL
├── requirements.txt       # Dependências
├── run_etl.bat           # Script Windows
└── README.md
```

## 🚀 Quick Start

### 1. Pré-requisitos

- Python 3.12+
- Docker Desktop
- Git

### 2. Setup do Ambiente

```bash
# Clone/navegue para o projeto
cd dre_pipeline

# Crie ambiente virtual
python -m venv venv

# Ative (Windows)
venv\Scripts\activate

# Ative (Linux/Mac)
source venv/bin/activate

# Instale dependências
pip install -r requirements.txt
```

### 3. Inicie o Banco de Dados

```bash
# Suba os containers (PostgreSQL + Adminer)
docker-compose up -d

# Verifique se está rodando
docker-compose ps
```

### 4. Configure o Projeto

```bash
# Copie o arquivo de configuração de exemplo
cp config.yml.example config.yml

# Edite se necessário (credenciais padrão já funcionam com Docker)
```

### 5. Crie as Tabelas

```bash
# Conecte ao banco e execute os scripts SQL
# Via Adminer: http://localhost:8080
# Servidor: postgres | Usuario: dre_user | Senha: dre_pass | Banco: dre_db

# Ou via linha de comando
docker exec -i dre_postgres psql -U dre_user -d dre_db < sql/01_create_schemas.sql
docker exec -i dre_postgres psql -U dre_user -d dre_db < sql/02_create_raw_tables.sql
docker exec -i dre_postgres psql -U dre_user -d dre_db < sql/03_create_stg_tables.sql
docker exec -i dre_postgres psql -U dre_user -d dre_db < sql/04_create_dw_tables.sql
docker exec -i dre_postgres psql -U dre_user -d dre_db < sql/05_create_audit_tables.sql
```

### 6. Execute o Pipeline

```bash
# Via script (recomendado)
run_etl.bat

# Ou diretamente
python -m etl._05_run_pipeline
```

### 7. Inicie a API

```bash
# Inicie o servidor
uvicorn api.main:app --reload --port 8000

# Acesse a documentação
# http://localhost:8000/docs
```

## 📊 Arquitetura de Dados

### Camada Bronze (RAW)

Dados brutos extraídos do Excel sem transformação:

| Tabela | Origem | Descrição |
|--------|--------|-----------|
| `raw.receita` | Receita_Realizado + Receita_Orç | Receitas por unidade/mês |
| `raw.despesa` | Despesas_Realizado + Despesas_Orç | Despesas por pacote/mês |
| `raw.dre` | Modelo DRE | Linhas da DRE |
| `raw.aliquota` | Aliquotas | Impostos por tipo |

### Camada Silver (STG)

Dados limpos e normalizados:

| Tabela | Transformações |
|--------|----------------|
| `stg.receita` | Unpivot meses, padronização nomes |
| `stg.despesa` | Unpivot meses, limpeza pacotes |
| `stg.dre` | Unpivot meses, categorização |
| `stg.aliquota` | Normalização percentuais |

### Camada Gold (DW)

Star Schema otimizado para análise:

**Dimensões:**
- `dim_calendario` - Meses do ano
- `dim_unidade` - Unidades de negócio
- `dim_tipo_receita` - SALES, SERVICE
- `dim_cenario` - Realizado, Orçado
- `dim_pacote` - Pacotes de despesas
- `dim_linha_dre` - Linhas da DRE

**Fatos:**
- `fact_receita` - Receitas mensais
- `fact_despesa` - Despesas mensais
- `fact_dre` - DRE consolidada
- `fact_aliquota` - Alíquotas de impostos

## 🔍 Data Quality

O pipeline inclui 12 validações automáticas:

| Regra | Descrição | Threshold |
|-------|-----------|-----------|
| `receita_realizado_total` | Soma receita realizada | ~67.6M |
| `receita_orcado_total` | Soma receita orçada | ~68.4M |
| `despesas_realizado_total` | Soma despesas realizadas | ~-41.6M |
| `lucro_liquido_dre` | Lucro líquido DRE | ~18.6M |
| `all_months_present` | 12 meses na dim_calendario | 12 |
| `fact_receita_not_empty` | fact_receita tem dados | >0 |
| `service_greater_sales` | SERVICE > SALES | True |

## 🌐 API Endpoints

### Ingestão (entrada de dados)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| POST | `/api/v1/upload` | **Upload de Excel** → dispara pipeline |
| POST | `/api/v1/pipeline/run` | Executa pipeline manualmente |

### Consulta (leitura de dados)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/api/v1/health` | Status do sistema |
| GET | `/api/v1/dre` | Resumo DRE (totais anuais) |
| GET | `/api/v1/dre/mensal` | DRE detalhado por mês |
| GET | `/api/v1/receita` | Receitas por tipo/cenário |
| GET | `/api/v1/despesa` | Top despesas por pacote |

### Exemplo de Uso

```bash
# Upload de novo arquivo Excel
curl -X POST "http://localhost:8000/api/v1/upload" \
     -F "file=@dados_case_pbi.xlsx"

# Consultar DRE
curl "http://localhost:8000/api/v1/dre"
```

## ⏰ Agendamento (Windows Task Scheduler)

1. Abra o **Agendador de Tarefas**
2. Criar Tarefa Básica
3. Configure:
   - **Trigger:** Diário às 06:00
   - **Ação:** Iniciar programa
   - **Programa:** `C:\...\dre_pipeline\run_etl.bat`
   - **Argumentos:** `auto`
   - **Iniciar em:** `C:\...\dre_pipeline`

## 📁 Dados de Origem

O arquivo fonte é `dados_case_pbi.xlsx` com 6 abas:

| Aba | Linhas | Colunas | Descrição |
|-----|--------|---------|-----------|
| Receita_Realizado | 89 | 14 | Receitas realizadas |
| Receita_Orç | 90 | 14 | Receitas orçadas |
| Despesas_Realizado | 46 | 15 | Despesas realizadas |
| Despesas_Orç | 46 | 15 | Despesas orçadas |
| Modelo DRE | 98 | 17 | Estrutura DRE |
| Aliquotas | 17 | 3 | Alíquotas impostos |

## 🔧 Desenvolvimento

```bash
# Testes
pytest tests/

# Formatação
black etl/ api/
isort etl/ api/

# Type checking
mypy etl/ api/
```

## 📝 Logs

Os logs são salvos em `logs/` com formato:
```
etl_YYYYMMDD_HHMMSS.log
```

## 🐳 Docker Services

| Serviço | Porta | Descrição |
|---------|-------|-----------|
| postgres | 5432 | PostgreSQL 15 |
| adminer | 8080 | UI Admin SQL |

## 📞 Suporte

Para dúvidas ou problemas, consulte a documentação em `docs/` ou abra uma issue.

---

**Versão:** 1.0.0  
**Última atualização:** 2025
