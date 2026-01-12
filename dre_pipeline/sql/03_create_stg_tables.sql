-- =============================================================================
-- DRE Analytics 2025
-- 03_create_stg_tables.sql
-- =============================================================================
-- Descrição: Tabelas da camada STG (Silver) - dados limpos e validados
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Tabela: stg.receita
-- Descrição: Receitas limpas e padronizadas
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS stg.receita CASCADE;

CREATE TABLE stg.receita (
    id                  SERIAL PRIMARY KEY,
    cenario             VARCHAR(20) NOT NULL,
    tipo_receita        VARCHAR(20) NOT NULL,      -- SALES, SERVICE
    unidade             VARCHAR(100) NOT NULL,
    mes_num             INTEGER NOT NULL,          -- 1-12
    mes_nome            VARCHAR(10) NOT NULL,      -- JAN, FEV, etc
    data_key            VARCHAR(10) NOT NULL,      -- 2025-01, etc
    valor               DECIMAL(18,2) NOT NULL,
    -- Metadados
    raw_created_at      TIMESTAMPTZ,
    stg_loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE stg.receita IS 'Receitas limpas - camada Silver';

CREATE INDEX idx_stg_receita_cenario ON stg.receita(cenario);
CREATE INDEX idx_stg_receita_data_key ON stg.receita(data_key);
CREATE INDEX idx_stg_receita_tipo ON stg.receita(tipo_receita);

-- -----------------------------------------------------------------------------
-- Tabela: stg.despesa
-- Descrição: Despesas limpas e padronizadas
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS stg.despesa CASCADE;

CREATE TABLE stg.despesa (
    id                  SERIAL PRIMARY KEY,
    cenario             VARCHAR(20) NOT NULL,
    unidade             VARCHAR(100) NOT NULL,
    pacote              VARCHAR(100) NOT NULL,
    conta               VARCHAR(255),
    mes_num             INTEGER NOT NULL,
    mes_nome            VARCHAR(10) NOT NULL,
    data_key            VARCHAR(10) NOT NULL,
    valor               DECIMAL(18,2) NOT NULL,
    -- Metadados
    raw_created_at      TIMESTAMPTZ,
    stg_loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE stg.despesa IS 'Despesas limpas - camada Silver';

CREATE INDEX idx_stg_despesa_cenario ON stg.despesa(cenario);
CREATE INDEX idx_stg_despesa_data_key ON stg.despesa(data_key);
CREATE INDEX idx_stg_despesa_pacote ON stg.despesa(pacote);

-- -----------------------------------------------------------------------------
-- Tabela: stg.dre
-- Descrição: Linhas DRE limpas e com hierarquia
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS stg.dre CASCADE;

CREATE TABLE stg.dre (
    id                  SERIAL PRIMARY KEY,
    linha_dre           VARCHAR(100) NOT NULL,
    categoria           VARCHAR(50) NOT NULL,
    ordem               INTEGER NOT NULL,
    nivel               INTEGER NOT NULL,          -- 1=Total, 2=Subtotal, 3=Detalhe
    mes_num             INTEGER NOT NULL,
    mes_nome            VARCHAR(10) NOT NULL,
    data_key            VARCHAR(10) NOT NULL,
    valor               DECIMAL(18,2) NOT NULL,
    -- Metadados
    raw_created_at      TIMESTAMPTZ,
    stg_loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE stg.dre IS 'Modelo DRE limpo - camada Silver';

CREATE INDEX idx_stg_dre_linha ON stg.dre(linha_dre);
CREATE INDEX idx_stg_dre_data_key ON stg.dre(data_key);

-- -----------------------------------------------------------------------------
-- Tabela: stg.aliquota
-- Descrição: Alíquotas limpas
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS stg.aliquota CASCADE;

CREATE TABLE stg.aliquota (
    id                  SERIAL PRIMARY KEY,
    tipo_imposto        VARCHAR(100) NOT NULL,
    mes_num             INTEGER NOT NULL,
    mes_nome            VARCHAR(10) NOT NULL,
    data_key            VARCHAR(10) NOT NULL,
    aliquota            DECIMAL(10,6) NOT NULL,
    -- Metadados
    raw_created_at      TIMESTAMPTZ,
    stg_loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE stg.aliquota IS 'Alíquotas limpas - camada Silver';

CREATE INDEX idx_stg_aliquota_tipo ON stg.aliquota(tipo_imposto);
CREATE INDEX idx_stg_aliquota_data_key ON stg.aliquota(data_key);
