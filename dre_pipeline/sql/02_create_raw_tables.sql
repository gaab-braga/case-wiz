-- =============================================================================
-- DRE Analytics 2025
-- 02_create_raw_tables.sql
-- =============================================================================
-- Descrição: Tabelas da camada RAW (Bronze) - dados brutos do Excel
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Tabela: raw.receita
-- Descrição: Dados brutos de receita (Realizado e Orçado)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS raw.receita CASCADE;

CREATE TABLE raw.receita (
    id                  SERIAL PRIMARY KEY,
    cenario             VARCHAR(20) NOT NULL,      -- REALIZADO, ORCADO
    tipo_receita        VARCHAR(50),               -- SALES, SERVICE, CONSOLIDADO
    unidade             VARCHAR(100),              -- Nome da unidade/cliente
    mes                 VARCHAR(10),               -- JAN, FEV, etc
    valor               DECIMAL(18,2),
    -- Metadados de ingestão
    source_file         VARCHAR(255),
    source_sheet        VARCHAR(100),
    source_row          INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    batch_id            VARCHAR(50)
);

COMMENT ON TABLE raw.receita IS 'Dados brutos de receita extraídos do Excel';

CREATE INDEX idx_raw_receita_cenario ON raw.receita(cenario);
CREATE INDEX idx_raw_receita_mes ON raw.receita(mes);
CREATE INDEX idx_raw_receita_batch ON raw.receita(batch_id);

-- -----------------------------------------------------------------------------
-- Tabela: raw.despesa
-- Descrição: Dados brutos de despesas (Realizado e Orçado)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS raw.despesa CASCADE;

CREATE TABLE raw.despesa (
    id                  SERIAL PRIMARY KEY,
    cenario             VARCHAR(20) NOT NULL,      -- REALIZADO, ORCADO
    data                DATE,
    unidade             VARCHAR(100),
    pacote              VARCHAR(100),
    conta               VARCHAR(255),
    valor               DECIMAL(18,2),
    -- Metadados de ingestão
    source_file         VARCHAR(255),
    source_sheet        VARCHAR(100),
    source_row          INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    batch_id            VARCHAR(50)
);

COMMENT ON TABLE raw.despesa IS 'Dados brutos de despesas extraídos do Excel';

CREATE INDEX idx_raw_despesa_cenario ON raw.despesa(cenario);
CREATE INDEX idx_raw_despesa_data ON raw.despesa(data);
CREATE INDEX idx_raw_despesa_batch ON raw.despesa(batch_id);

-- -----------------------------------------------------------------------------
-- Tabela: raw.dre
-- Descrição: Dados brutos do Modelo DRE
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS raw.dre CASCADE;

CREATE TABLE raw.dre (
    id                  SERIAL PRIMARY KEY,
    linha_dre           VARCHAR(100) NOT NULL,     -- Nome da linha (Receita Bruta, EBITDA, etc)
    categoria           VARCHAR(50),               -- Categoria (Receita, Custo, etc)
    mes                 VARCHAR(10),               -- JAN, FEV, etc
    valor               DECIMAL(18,2),
    ordem               INTEGER,                   -- Ordem de exibição
    -- Metadados de ingestão
    source_file         VARCHAR(255),
    source_sheet        VARCHAR(100),
    source_row          INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    batch_id            VARCHAR(50)
);

COMMENT ON TABLE raw.dre IS 'Dados brutos do modelo DRE extraídos do Excel';

CREATE INDEX idx_raw_dre_linha ON raw.dre(linha_dre);
CREATE INDEX idx_raw_dre_batch ON raw.dre(batch_id);

-- -----------------------------------------------------------------------------
-- Tabela: raw.aliquota
-- Descrição: Dados brutos de alíquotas de imposto
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS raw.aliquota CASCADE;

CREATE TABLE raw.aliquota (
    id                  SERIAL PRIMARY KEY,
    tipo_imposto        VARCHAR(100) NOT NULL,     -- Imposto Sobre Faturamento, IR & CSLL
    mes                 VARCHAR(10),               -- JAN, FEV, etc
    aliquota            DECIMAL(10,6),             -- Valor da alíquota (ex: -0.0826)
    -- Metadados de ingestão
    source_file         VARCHAR(255),
    source_sheet        VARCHAR(100),
    source_row          INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    batch_id            VARCHAR(50)
);

COMMENT ON TABLE raw.aliquota IS 'Dados brutos de alíquotas extraídos do Excel';

CREATE INDEX idx_raw_aliquota_tipo ON raw.aliquota(tipo_imposto);
CREATE INDEX idx_raw_aliquota_batch ON raw.aliquota(batch_id);
