# 📊 Análises Exploratórias - DRE 2025

## Estrutura dos Notebooks

Este diretório contém análises exploratórias (EDA) **separadas por tema de negócio**.
A construção de dados para produção está no `dre_pipeline/`.

### 📁 Notebooks Disponíveis

| # | Notebook | Descrição | Stakeholder |
|---|----------|-----------|-------------|
| 1 | [01_eda_visao_geral.ipynb](01_eda_visao_geral.ipynb) | Estrutura do Excel, qualidade, estatísticas básicas | TI / Dados |
| 2 | [02_eda_receitas_despesas.ipynb](02_eda_receitas_despesas.ipynb) | Análise de receitas por unidade/tipo, despesas por pacote | Financeiro |
| 3 | [03_eda_dre_kpis.ipynb](03_eda_dre_kpis.ipynb) | DRE consolidada, margens, KPIs executivos | Diretoria |
| 4 | [eda_completa_v2.ipynb](eda_completa_v2.ipynb) | Notebook original completo (backup) | - |

---

## 🎯 Principais Insights

### 💰 Receitas (2025)
- **Receita Bruta Total:** R$ 67,6M (Realizado) vs R$ 68,4M (Orçado)
- **SERVICE > SALES:** Serviços representam ~60% da receita
- **Principais unidades:** SJC, SAO e BSB lideram em faturamento

### 📉 Despesas (2025)
- **Despesas Realizadas:** R$ 41,6M vs R$ 48,8M orçadas
- **Economia:** R$ 7,2M (~15% abaixo do orçado)
- **Principais pacotes:** Custos com pessoal (~40%), infraestrutura (~20%)

### 📊 DRE — Indicadores-chave
| KPI | Valor | Meta | Status |
|-----|-------|------|--------|
| Margem EBITDA | 34,4% | 30%+ | ✅ Acima |
| Margem Líquida | 27,5% | 25%+ | ✅ Acima |
| Lucro Líquido | R$ 18,6M | - | ✅ Positivo |

---

## 📐 Arquitetura de Dados

A análise identificou a seguinte estrutura Star Schema:

```
                    ┌─────────────┐
                    │ dCalendario │
                    │  (12 meses) │
                    └──────┬──────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│   fReceita    │  │   fDespesa    │  │     fDRE      │
│  (2.112 lin)  │  │  (5.280 lin)  │  │   (84 lin)    │
└───────────────┘  └───────────────┘  └───────────────┘
        │                  │
        ▼                  ▼
┌───────────────┐  ┌───────────────┐
│ dTipoReceita  │  │   dPacote     │
│ dUnidade      │  │   dUnidade    │
│ dCenario      │  │   dCenario    │
└───────────────┘  └───────────────┘
```

---

## ⚠️ Observações Importantes

1. **Estes notebooks destinam-se apenas a análises exploratórias (EDA)** — não geram os conjuntos finais de dados para o Power BI
2. **Dados de produção** são gerados pelo `dre_pipeline/` 
3. **Para reprocessar dados**, use: `python -m etl._05_run_pipeline`
4. **CSVs finais** estão em: `02_dados_tratados/powerbi_model/`

---

## 🔄 Atualização

- **Última análise:** Janeiro 2025
- **Dados fonte:** `01_dados_originais/dados_case_pbi.xlsx`
- **Pipeline ETL:** `dre_pipeline/`
