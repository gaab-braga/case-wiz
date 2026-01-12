# 🏗️ ARQUITETURA & ENGENHARIA DE DADOS - DASHBOARD DRE 2025

> **Documento Consolidado** | Versão 1.0 | Janeiro 2025  
> **Autor:** Engenharia de Dados  
> **Status:** ✅ Produção

---

## 1. VISÃO GERAL DA ARQUITETURA

### 1.1 Stack Tecnológico

| Camada | Tecnologia | Versão | Propósito |
|--------|------------|--------|-----------|
| **Extração** | Python + Pandas | 3.11+ | ETL robusto |
| **Armazenamento** | PostgreSQL | 15+ | Data Warehouse |
| **Orquestração** | Docker Compose | 2.0+ | Containerização |
| **API** | FastAPI | 0.100+ | Endpoints REST |
| **Visualização** | Power BI Desktop | Atual | Dashboards |
| **Versionamento** | Git | - | Controle de versão |

### 1.2 Fluxo de Dados

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PIPELINE ETL - MEDALION                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   📥 FONTE              🥉 BRONZE           🥈 SILVER          🥇 GOLD       │
│   ────────              ──────────          ─────────          ────────      │
│                                                                              │
│   Excel                 raw_receita         stg_receita        fReceita     │
│   (5 abas)        ──►   raw_despesa    ──►  stg_despesa   ──►  fDespesa     │
│                         raw_dre             stg_dre            fDRE         │
│                         raw_aliquota        stg_aliquota       fAliquota    │
│                                                                              │
│   📊 Formato:           Formato:            Formato:           Formato:     │
│   Report                Cópia exata         Unpivot +          Star Schema  │
│   (Colunas mês)         (Preservado)        Normalizado        (Otimizado)  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. MODELO DIMENSIONAL (STAR SCHEMA)

### 2.1 Diagrama

```
                              ┌─────────────────────┐
                              │    dCalendario      │
                              │   ─────────────────  │
                              │ PK Data_Key         │
                              │    Ano              │
                              │    Mes_Num          │
                              │    Mes_Nome         │
                              │    Trimestre        │
                              └──────────┬──────────┘
                                         │
           ┌────────────────┬────────────┼────────────┬─────────────────┐
           ▼                ▼            ▼            ▼                 ▼
  ┌─────────────────┐ ┌───────────┐ ┌─────────┐ ┌───────────┐ ┌────────────────┐
  │   fReceita      │ │ fDespesa  │ │  fDRE   │ │fAliquota  │ │ fDRE_por_Area  │
  │ ───────────     │ │ ───────── │ │ ─────── │ │ ───────── │ │ ───────────── │
  │FK Data_Key      │ │FK Data_Key│ │FK Data  │ │FK Data_Key│ │FK Data_Key    │
  │FK Unidade       │ │FK Unidade │ │FK Linha │ │FK Tipo    │ │FK Area        │
  │FK Tipo_Receita  │ │FK Pacote  │ │FK Cen.  │ │   Aliquota│ │FK Linha_DRE   │
  │FK Cenario       │ │FK Cenario │ │   Valor │ └───────────┘ │   Valor       │
  │   Valor         │ │   Valor   │ │   Ordem │               └────────────────┘
  └────────┬────────┘ └─────┬─────┘ └────┬────┘
           │                │            │
           ▼                ▼            ▼
  ┌─────────────────┐ ┌───────────┐ ┌────────────────┐ ┌───────────┐
  │ dTipoReceita    │ │ dPacote   │ │  dLinhaDRE     │ │ dCenario  │
  │ ───────────     │ │ ───────── │ │ ───────────    │ │ ───────── │
  │PK Tipo_Receita  │ │PK Pacote  │ │PK Linha_DRE    │ │PK Cenario │
  │   Descricao     │ │   Grupo   │ │   Linha_Pai    │ │   Codigo  │
  └─────────────────┘ └───────────┘ │   Nivel        │ └───────────┘
                                    │   Ordem_DRE    │
  ┌─────────────────┐               └────────────────┘
  │   dUnidade      │
  │ ───────────     │
  │PK Unidade       │
  │   Regiao        │
  │   Nome_Completo │
  └─────────────────┘
```

### 2.2 Tabelas de Dimensão

| Dimensão | PK | Registros | Descrição |
|----------|-----|-----------|-----------|
| **dCalendario** | Data_Key (YYYYMM) | 12 | Meses de 2025 |
| **dCenario** | Cenario | 2 | Realizado, Orçado |
| **dTipoReceita** | Tipo_Receita | 2 | SALES, SERVICE |
| **dUnidade** | Unidade | 8 | HQ, SAO, SJC, BSB, CTB, POA, FLN, FOR |
| **dPacote** | Pacote | 13 | Categorias de despesa |
| **dLinhaDRE** | Linha_DRE | 25 | Linhas com hierarquia Parent-Child |

### 2.3 Tabelas de Fato

| Fato | FKs | Registros | Granularidade |
|------|-----|-----------|---------------|
| **fReceita** | Data, Unidade, Tipo, Cenario | 1.571 | Mês × Unidade × Tipo × Cenário |
| **fDespesa** | Data, Unidade, Pacote, Cenario | 40.472 | Mês × Unidade × Pacote × Cenário |
| **fDRE** | Data, Linha, Cenario | 602 | Mês × Linha × Cenário |
| **fAliquota** | Data, Tipo_Imposto | 24 | Mês × Tipo Imposto |
| **fDRE_por_Area** | Data, Area, Linha, Cenario | 1.200 | Mês × Área × Linha × Cenário |

---

## 3. ESTRUTURA DE ARQUIVOS

### 3.1 Dados Tratados (Gold)

```
02_dados_tratados/
└── powerbi_model/
    ├── dCalendario.csv      # 12 meses
    ├── dCenario.csv         # 2 cenários
    ├── dLinhaDRE.csv        # 25 linhas hierárquicas
    ├── dPacote.csv          # 13 pacotes despesa
    ├── dTipoReceita.csv     # SALES/SERVICE
    ├── dUnidade.csv         # 8 unidades
    ├── fAliquota.csv        # 24 alíquotas
    ├── fDespesa.csv         # 40k+ registros
    ├── fDRE.csv             # 602 registros
    ├── fDRE_por_Area.csv    # 1200 registros
    └── fReceita.csv         # 1571 registros
```

### 3.2 Scripts de Análise

```
03_analise/
├── extracao_robusta_dre.py           # Extração DRE do Excel
├── criar_dre_por_area.py             # Gera fDRE_por_Area
├── investigar_anomalia_marco.py      # Análise PLR
├── validar_extracao_dre.py           # Validação cruzada
├── reconstruir_dados_completo.py     # Rebuild completo
└── *.ipynb                           # Notebooks EDA
```

### 3.3 Pipeline ETL

```
projeto/etl/
├── _00_config.py           # Configurações
├── _01_load_files.py       # Carga de arquivos
├── _02_transform_raw_to_stg.py    # Bronze → Silver
├── _03_transform_stg_to_dw.py     # Silver → Gold
├── _04_dq_checks.py        # Validações de qualidade
└── _05_run_pipeline.py     # Orquestrador
```

---

## 4. RELACIONAMENTOS POWER BI

### 4.1 Configuração de Relacionamentos

```
fReceita[Data_Key]      ──► dCalendario[Data_Key]     (N:1)
fReceita[Cenario]       ──► dCenario[Cenario]         (N:1)
fReceita[Tipo_Receita]  ──► dTipoReceita[Tipo_Receita] (N:1)
fReceita[Unidade]       ──► dUnidade[Unidade]         (N:1)

fDespesa[Data_Key]      ──► dCalendario[Data_Key]     (N:1)
fDespesa[Cenario]       ──► dCenario[Cenario]         (N:1)
fDespesa[Pacote]        ──► dPacote[Pacote]           (N:1)
fDespesa[Unidade]       ──► dUnidade[Unidade]         (N:1)

fDRE[Data_Key]          ──► dCalendario[Data_Key]     (N:1)
fDRE[Cenario]           ──► dCenario[Cenario]         (N:1)
fDRE[Linha_DRE]         ──► dLinhaDRE[Linha_DRE]      (N:1)

fAliquota[Data_Key]     ──► dCalendario[Data_Key]     (N:1)
```

### 4.2 Hierarquia Parent-Child (dLinhaDRE)

```dax
// Caminho da Hierarquia
Caminho_DRE = 
PATH(dLinhaDRE[Linha_DRE], dLinhaDRE[Linha_Pai])

// Nível na Hierarquia
Nivel_Hierarquia = 
PATHLENGTH([Caminho_DRE])

// Nome do Pai
Nome_Pai = 
LOOKUPVALUE(
    dLinhaDRE[Linha_DRE],
    dLinhaDRE[Linha_DRE],
    PATHITEM([Caminho_DRE], PATHLENGTH([Caminho_DRE])-1)
)
```

---

## 5. QUALIDADE DE DADOS

### 5.1 Validações Implementadas

| Tipo | Validação | Status |
|------|-----------|--------|
| **Integridade** | FKs válidas em todas as tabelas | ✅ |
| **Completude** | Sem valores NULL em campos obrigatórios | ✅ |
| **Consistência** | Receita fDRE = SUM(fReceita) | ✅ |
| **Precisão** | EBITDA calculado = Receita - Custos | ✅ |
| **Unicidade** | Sem duplicatas em chaves compostas | ✅ |

### 5.2 Regras de Negócio

```python
# Validação: Receita DRE = Soma das Receitas
assert abs(fDRE['Receita Bruta'].sum() - fReceita['Valor'].sum()) < 0.01

# Validação: EBITDA = Receita Líquida - Custos Operacionais
ebitda_calc = receita_liquida - custos_operacionais
assert abs(ebitda_dre - ebitda_calc) < 0.01

# Validação: Margem EBITDA dentro de limites razoáveis
assert 0 < margem_ebitda < 1
```

---

## 6. INFRAESTRUTURA

### 6.1 Docker Compose

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: dre_dw
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - pgdata:/var/lib/postgresql/data
    ports:
      - "5432:5432"
  
  api:
    build: ./api
    ports:
      - "8000:8000"
    depends_on:
      - postgres

volumes:
  pgdata:
```

### 6.2 API REST (FastAPI)

| Endpoint | Método | Descrição |
|----------|--------|-----------|
| `/api/v1/dre` | GET | DRE consolidada |
| `/api/v1/dre/{mes}` | GET | DRE por mês |
| `/api/v1/receitas` | GET | Receitas |
| `/api/v1/despesas` | GET | Despesas |
| `/api/v1/kpis` | GET | Indicadores |
| `/api/v1/health` | GET | Status do sistema |

### 6.3 Execução do Pipeline

```bash
# Windows
cd projeto
run_etl.bat

# Linux/Mac
cd projeto
./run_etl.sh

# Ou manualmente
python -m etl._05_run_pipeline
```

---

## 7. SCHEMAS SQL

### 7.1 Estrutura de Schemas

```sql
-- Bronze: Dados brutos
CREATE SCHEMA bronze;

-- Silver: Dados limpos
CREATE SCHEMA silver;

-- Gold: Star Schema
CREATE SCHEMA gold;

-- Auditoria
CREATE SCHEMA audit;
```

### 7.2 Exemplo: Tabela Fato Receita

```sql
CREATE TABLE gold.fact_receita (
    id SERIAL PRIMARY KEY,
    data_key VARCHAR(6) NOT NULL,
    unidade VARCHAR(10) NOT NULL,
    tipo_receita VARCHAR(20) NOT NULL,
    cenario VARCHAR(20) NOT NULL,
    valor DECIMAL(15,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (data_key) REFERENCES gold.dim_calendario(data_key),
    FOREIGN KEY (unidade) REFERENCES gold.dim_unidade(unidade),
    FOREIGN KEY (tipo_receita) REFERENCES gold.dim_tipo_receita(tipo_receita),
    FOREIGN KEY (cenario) REFERENCES gold.dim_cenario(cenario)
);

CREATE INDEX idx_fact_receita_data ON gold.fact_receita(data_key);
CREATE INDEX idx_fact_receita_cenario ON gold.fact_receita(cenario);
```

---

## 8. BOAS PRÁTICAS IMPLEMENTADAS

### 8.1 Engenharia de Dados

| Prática | Implementação |
|---------|---------------|
| **Medallion Architecture** | Bronze → Silver → Gold |
| **Star Schema** | 5 dimensões + 5 fatos |
| **Idempotência** | Pipeline pode re-executar sem duplicatas |
| **Logging** | Logs estruturados em `logs/` |
| **Configuração externa** | `config.yml` para parâmetros |
| **Testes** | Validações em `tests/` |

### 8.2 Naming Conventions

| Tipo | Padrão | Exemplo |
|------|--------|---------|
| Dimensões | `d{Nome}` | dCalendario, dCenario |
| Fatos | `f{Nome}` | fReceita, fDRE |
| Chaves | `{Entidade}_Key` | Data_Key |
| Medidas | PascalCase descritivo | Receita Bruta, Margem EBITDA |

---

## 9. MANUTENÇÃO

### 9.1 Comandos Úteis

```bash
# Verificar qualidade dos dados
python -m etl._04_dq_checks

# Rebuild completo
python 03_analise/reconstruir_dados_completo.py

# Validar DRE
python 03_analise/validar_extracao_dre.py

# Investigar anomalias
python 03_analise/investigar_anomalia_marco.py
```

### 9.2 Troubleshooting

| Problema | Causa Provável | Solução |
|----------|----------------|---------|
| Receita não bate | Filtro de cenário | Verificar dCenario |
| EBITDA incorreto | Linha errada | Verificar dLinhaDRE |
| Dados duplicados | Re-execução sem limpeza | Rodar DQ checks |
| FK inválida | Dimensão incompleta | Verificar CSVs de dimensão |

---

*Documento consolidado de: 02_ARQUITETURA_ENGENHARIA.md*
