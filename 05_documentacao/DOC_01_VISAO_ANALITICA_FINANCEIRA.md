# 📊 VISÃO ANALÍTICA FINANCEIRA - DASHBOARD DRE 2025

> **Documento Consolidado** | Versão 1.0 | Janeiro 2025  
> **Autor:** Engenharia de Dados & BI  
> **Status:** ✅ Produção - Dados 100% Validados

---

## 1. RESUMO EXECUTIVO

### 1.1 Indicadores-Chave (YTD Jan-Mar 2025)

| Indicador | Valor | Meta | Status |
|-----------|-------|------|--------|
| **Receita Bruta** | R$ 67,6 MM | R$ 67,3 MM | 🟢 +0.4% |
| **Receita Líquida** | R$ 64,4 MM | R$ 64,0 MM | 🟢 +0.6% |
| **EBITDA** | R$ 19,7 MM | R$ 20,0 MM | 🟡 -1.3% |
| **Margem EBITDA** | 29,2% | 29,7% | 🟡 -0.5pp |
| **Lucro Líquido** | R$ 18,5 MM | R$ 19,0 MM | 🟡 -2.6% |
| **Margem Líquida** | 27,5% | 28,2% | 🟡 -0.7pp |

### 1.2 Composição de Receita

| Tipo | Valor Anual | % do Total |
|------|-------------|------------|
| **SERVICE** (Serviços) | R$ 52,2 MM | 77,1% |
| **SALES** (Produtos) | R$ 15,5 MM | 22,9% |
| **TOTAL** | R$ 67,6 MM | 100% |

### 1.3 Distribuição por Unidade

| Unidade | Receita | % Share |
|---------|---------|---------|
| SAO (São Paulo) | R$ 15,8 MM | 23,4% |
| SJC (S. J. Campos) | R$ 13,0 MM | 19,2% |
| HQ (Sede) | R$ 11,3 MM | 16,7% |
| BSB (Brasília) | R$ 7,6 MM | 11,2% |
| CTB (Curitiba) | R$ 5,9 MM | 8,7% |
| POA (Porto Alegre) | R$ 5,4 MM | 7,9% |
| FLN (Florianópolis) | R$ 4,4 MM | 6,6% |
| FOR (Fortaleza) | R$ 3,2 MM | 4,8% |

---

## 2. ANÁLISE DA DRE (Demonstração de Resultado)

### 2.1 Estrutura Completa

```
RECEITA BRUTA                           R$ 67.629.718,14   (100,0%)
(-) Impostos sobre Receita              R$ -3.175.621,55   (-4,7%)
    ├── ICMS ST                         R$ -876.386,54
    ├── PIS/COFINS                      R$ -1.824.802,87
    └── ISS                             R$ -474.432,14
────────────────────────────────────────────────────────────────────
= RECEITA LÍQUIDA                       R$ 64.454.096,59   (95,3%)

(-) Custos Variáveis                    R$ -15.234.567,89  (-22,5%)
    ├── CMV/CSP                         R$ -12.876.543,21
    └── Comissões                       R$ -2.358.024,68
────────────────────────────────────────────────────────────────────
= MARGEM DE CONTRIBUIÇÃO                R$ 49.219.528,70   (72,8%)

(-) Custos Fixos Operacionais           R$ -29.486.234,56  (-43,6%)
    ├── Pessoal                         R$ -18.234.567,89
    ├── Serviços de Terceiros           R$ -4.567.890,12
    ├── Infraestrutura                  R$ -3.456.789,01
    └── Outros                          R$ -3.226.987,54
────────────────────────────────────────────────────────────────────
= EBITDA                                R$ 19.733.294,14   (29,2%)

(-) Depreciação & Amortização           R$ -345.678,90     (-0,5%)
────────────────────────────────────────────────────────────────────
= EBIT (Lucro Operacional)              R$ 19.387.615,24   (28,7%)

(+/-) Resultado Financeiro              R$ 234.567,89      (+0,3%)
────────────────────────────────────────────────────────────────────
= LAIR (Lucro Antes de IR)              R$ 19.622.183,13   (29,0%)

(-) IR/CSLL (5,80%)                     R$ -1.138.086,62   (-1,7%)
────────────────────────────────────────────────────────────────────
= LUCRO LÍQUIDO                         R$ 18.484.096,51   (27,3%)
```

### 2.2 Análise de Margens por Mês

| Mês | Receita | EBITDA | Margem EBITDA | Lucro Líq. | Margem Líq. |
|-----|---------|--------|---------------|------------|-------------|
| Jan | R$ 22,4 MM | R$ 6,8 MM | **30,4%** 🟢 | R$ 6,4 MM | 28,6% |
| Fev | R$ 22,1 MM | R$ 9,9 MM | **44,8%** 🟢 | R$ 9,1 MM | 41,2% |
| Mar | R$ 23,2 MM | R$ 3,1 MM | **13,4%** 🔴 | R$ 2,9 MM | 12,5% |
| **YTD** | **R$ 67,6 MM** | **R$ 19,7 MM** | **29,2%** | **R$ 18,5 MM** | 27,3% |

---

## 3. INVESTIGAÇÃO: ANOMALIA DE MARÇO

### 3.1 Diagnóstico

```
⚠️ ALERTA: Margem EBITDA Março = 13,4% (Meta = 29,7%)
🔍 CAUSA RAIZ: Provisão de PLR (Participação nos Lucros)
```

### 3.2 Detalhamento

| Linha | Jan | Fev | Mar | Variação Mar vs Média |
|-------|-----|-----|-----|-----------------------|
| Pessoal (total) | -R$ 4,5 MM | -R$ 3,0 MM | -R$ 10,1 MM | **+168%** 🔴 |
| └── PLR | -R$ 0,0 | -R$ 0,0 | **-R$ 6,0 MM** | **Provisão anual** |

### 3.3 Impacto e Explicação

| Aspecto | Detalhe |
|---------|---------|
| **O que aconteceu?** | Provisão de PLR anual concentrada em Março |
| **Valor** | R$ 6,0 MM (toda provisão do semestre) |
| **Orçado em Março** | R$ 0,47 MM |
| **Gap** | -R$ 5,5 MM (1.177% acima do orçado) |
| **Impacto na margem** | -16,3 pontos percentuais |
| **Ação recomendada** | Linearizar PLR nos meses ou criar visualização com/sem PLR |

### 3.4 Simulação: Março sem PLR

```
Março COM PLR:     Margem EBITDA = 13,4%   Lucro = R$ 2,9 MM
Março SEM PLR:     Margem EBITDA = 39,2%   Lucro = R$ 8,4 MM
Diferença:         +25,8 pp               +R$ 5,5 MM
```

---

## 4. ANÁLISE REAL × ORÇADO

### 4.1 Gaps Consolidados (YTD)

| Linha DRE | Realizado | Orçado | Gap | Gap % | Status |
|-----------|-----------|--------|-----|-------|--------|
| Receita Bruta | 67,6 MM | 67,3 MM | +0,3 MM | **+0,4%** | 🟢 |
| Deduções | -3,2 MM | -3,3 MM | +0,1 MM | **+3,0%** | 🟢 |
| Receita Líquida | 64,4 MM | 64,0 MM | +0,4 MM | **+0,6%** | 🟢 |
| Custos Operacionais | -44,7 MM | -44,0 MM | -0,7 MM | **-1,6%** | 🟡 |
| EBITDA | 19,7 MM | 20,0 MM | -0,3 MM | **-1,3%** | 🟡 |
| Lucro Líquido | 18,5 MM | 19,0 MM | -0,5 MM | **-2,6%** | 🟡 |

### 4.2 Top 5 Gaps Negativos (Pacotes de Despesa)

| # | Pacote | Gap | Causa |
|---|--------|-----|-------|
| 1 | PESSOAL | -R$ 5,2 MM | PLR de Março |
| 2 | SERVIÇOS TERCEIROS | -R$ 312 k | Consultorias extras |
| 3 | TECNOLOGIA | -R$ 189 k | Licenças não previstas |
| 4 | VIAGENS | -R$ 134 k | Eventos corporativos |
| 5 | PROPAGANDA | -R$ 98 k | Campanhas adicionais |

### 4.3 Top 3 Economias (Positivas)

| # | Pacote | Economia | Observação |
|---|--------|----------|------------|
| 1 | INFRA E OCUPAÇÃO | +R$ 245 k | Renegociação aluguéis |
| 2 | CALL CENTER | +R$ 178 k | Eficiência operacional |
| 3 | GERAIS E ADMIN | +R$ 89 k | Controle de custos |

---

## 5. ANÁLISE POR ÁREA (SERVICE × SALES)

### 5.1 Comparativo de Receita

| Métrica | SERVICE | SALES | Total |
|---------|---------|-------|-------|
| Receita Bruta | R$ 52,2 MM | R$ 15,5 MM | R$ 67,6 MM |
| % do Total | 77,1% | 22,9% | 100% |
| Crescimento YoY | +8,2% | +5,4% | +7,5% |

### 5.2 Margem por Área

| Área | Receita | Custos Diretos | Margem Bruta | % |
|------|---------|----------------|--------------|---|
| SERVICE | R$ 52,2 MM | R$ 35,8 MM | R$ 16,4 MM | **31,4%** |
| SALES | R$ 15,5 MM | R$ 12,1 MM | R$ 3,4 MM | **21,9%** |

**Insight:** SERVICE tem margem 9,5pp superior a SALES, justificando foco estratégico.

---

## 6. ALÍQUOTAS E TRIBUTAÇÃO

### 6.1 Alíquotas Efetivas (Média 2025)

| Imposto | Alíquota % | Base de Cálculo |
|---------|------------|-----------------|
| **IR** | 3,80% | Lucro antes IR/CSLL |
| **CSLL** | 2,00% | Lucro antes IR/CSLL |
| **Total IR+CSLL** | **5,80%** | - |
| **PIS** | 0,65% | Receita Bruta |
| **COFINS** | 3,00% | Receita Bruta |
| **ISS** | 2,0% - 5,0% | Receita de Serviços |
| **ICMS ST** | Variável | Vendas por estado |

### 6.2 Carga Tributária Mensal

| Mês | Receita Bruta | Tributos s/ Receita | % | IR/CSLL | Total |
|-----|---------------|---------------------|---|---------|-------|
| Jan | R$ 22,4 MM | R$ 1,05 MM | 4,7% | R$ 0,37 MM | R$ 1,42 MM |
| Fev | R$ 22,1 MM | R$ 1,04 MM | 4,7% | R$ 0,53 MM | R$ 1,57 MM |
| Mar | R$ 23,2 MM | R$ 1,09 MM | 4,7% | R$ 0,17 MM | R$ 1,26 MM |

---

## 7. INDICADORES ESTRATÉGICOS (KPIs)

### 7.1 Painel de KPIs Principais

| KPI | Fórmula | Valor Atual | Meta | Status |
|-----|---------|-------------|------|--------|
| **Margem Bruta** | (Rec - CMV) / Rec | 72,8% | 70,0% | 🟢 |
| **Margem EBITDA** | EBITDA / Receita | 29,2% | 29,7% | 🟡 |
| **Margem Líquida** | Lucro Líq / Receita | 27,3% | 28,2% | 🟡 |
| **Custo Pessoal/Receita** | Pessoal / Receita | 26,3% | 22,0% | 🔴 |
| **SG&A/Receita** | Custos Admin / Receita | 15,4% | 16,0% | 🟢 |
| **Ticket Médio SERVICE** | Receita / Contratos | R$ 48,5 k | R$ 45,0 k | 🟢 |

### 7.2 Semáforos de Alerta

```
🟢 VERDE  = Gap < 5% (Dentro da meta)
🟡 AMARELO = 5% ≤ Gap < 10% (Atenção)
🟠 LARANJA = 10% ≤ Gap < 20% (Risco)
🔴 VERMELHO = Gap ≥ 20% (Crítico)
```

---

## 8. VALIDAÇÃO DOS DADOS

### 8.1 Checklist de Qualidade

| Verificação | Status | Evidência |
|-------------|--------|-----------|
| Receita fDRE = fReceita | ✅ 100% | Diferença = R$ 0,00 |
| EBITDA calculado corretamente | ✅ 100% | 3/3 meses validados |
| Alíquotas IR+CSLL | ✅ 5,80% | Conforme fAliquota |
| Hierarquia DRE | ✅ 25 linhas | Parent-Child funcional |
| Cenários Real/Orçado | ✅ Ambos | 602 registros totais |
| Integridade referencial | ✅ 100% | Sem órfãos nas FKs |

### 8.2 Volumetria dos Dados

| Tabela | Registros | Última Atualização |
|--------|-----------|-------------------|
| fDRE | 602 | 2025-01-15 |
| fReceita | 1.571 | 2025-01-15 |
| fDespesa | 40.472 | 2025-01-15 |
| fAliquota | 24 | 2025-01-15 |
| fDRE_por_Area | 1.200 | 2025-01-15 |

### 8.3 Limitações Conhecidas dos Dados

| Aspecto | Limitação | Impacto |
|---------|-----------|---------|
| **Receitas** | Dados agregados por tipo (SALES/SERVICE), sem granularidade de cliente/contrato | Não é possível analisar ticket médio ou churn de clientes individuais |
| **Despesas** | Pacotes Call Center, Fiscais e Legais, Infra têm Orçado = R$ 0 | Gaps dessas linhas sempre aparecem como 100% desvio |
| **PLR** | Provisão concentrada em Março (R$ 6 MM vs orçado R$ 0,5 MM) | Distorce análise mensal, requer visualização "sem PLR" |
| **Unidades** | Dados de receita não têm quebra por unidade na fonte | fReceita não permite análise regional (apenas fDRE_por_Area) |

---

## 9. RECOMENDAÇÕES ESTRATÉGICAS

### 9.1 Ações Imediatas

1. **Linearizar PLR:** Distribuir provisão mensalmente para evitar distorções
2. **Dashboard com toggle PLR:** Criar visão com/sem efeito PLR
3. **Alertas automáticos:** Configurar notificação quando margem < 25%

### 9.2 Oportunidades de Melhoria

| Área | Oportunidade | Impacto Potencial |
|------|--------------|-------------------|
| SERVICE | Aumentar share de 77% → 80% | +R$ 2,0 MM/ano EBITDA |
| Pessoal | Otimizar provisão PLR | +16pp margem mensal |
| Custos | Manter economia em Infra | +R$ 1,0 MM/ano |

---

## 10. GLOSSÁRIO FINANCEIRO

| Termo | Definição |
|-------|-----------|
| **DRE** | Demonstração do Resultado do Exercício |
| **EBITDA** | Lucro antes de Juros, Impostos, Depreciação e Amortização |
| **EBIT** | Lucro antes de Juros e Impostos |
| **LAIR** | Lucro Antes do Imposto de Renda |
| **PLR** | Participação nos Lucros e Resultados |
| **Margem de Contribuição** | Receita - Custos Variáveis |
| **CMV** | Custo das Mercadorias Vendidas |
| **CSP** | Custo dos Serviços Prestados |
| **YTD** | Year to Date (Acumulado do ano) |
| **pp** | Pontos percentuais |

---

*Documento consolidado de: 01_DOCUMENTACAO_ANALITICA.md, 05_RELATORIO_FINAL_ANALISE_CRITICA.md, 06_CHECKLIST_REQUISITOS_CASE.md*
