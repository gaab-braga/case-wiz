-- =============================================================================
-- DRE Analytics 2025
-- 04_create_dw_tables.sql
-- =============================================================================
-- Descrição: Tabelas da camada DW (Gold) - Star Schema para Power BI
-- =============================================================================

-- =============================================================================
-- DIMENSÕES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Tabela: dw.dim_calendario
-- Descrição: Dimensão de tempo (meses de 2025)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.dim_calendario CASCADE;

CREATE TABLE dw.dim_calendario (
    data_key            VARCHAR(10) PRIMARY KEY,   -- 2025-01, 2025-02, etc
    mes_num             INTEGER NOT NULL,
    mes_nome            VARCHAR(10) NOT NULL,
    mes_nome_completo   VARCHAR(20) NOT NULL,
    trimestre           VARCHAR(5) NOT NULL,       -- Q1, Q2, Q3, Q4
    semestre            VARCHAR(5) NOT NULL,       -- S1, S2
    ano                 INTEGER NOT NULL,
    dw_loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dw.dim_calendario IS 'Dimensão de calendário - meses de 2025';

-- Inserir dados estáticos de calendário
INSERT INTO dw.dim_calendario (data_key, mes_num, mes_nome, mes_nome_completo, trimestre, semestre, ano)
VALUES
    ('2025-01', 1, 'JAN', 'Janeiro', 'Q1', 'S1', 2025),
    ('2025-02', 2, 'FEV', 'Fevereiro', 'Q1', 'S1', 2025),
    ('2025-03', 3, 'MAR', 'Março', 'Q1', 'S1', 2025),
    ('2025-04', 4, 'ABR', 'Abril', 'Q2', 'S1', 2025),
    ('2025-05', 5, 'MAI', 'Maio', 'Q2', 'S1', 2025),
    ('2025-06', 6, 'JUN', 'Junho', 'Q2', 'S1', 2025),
    ('2025-07', 7, 'JUL', 'Julho', 'Q3', 'S2', 2025),
    ('2025-08', 8, 'AGO', 'Agosto', 'Q3', 'S2', 2025),
    ('2025-09', 9, 'SET', 'Setembro', 'Q3', 'S2', 2025),
    ('2025-10', 10, 'OUT', 'Outubro', 'Q4', 'S2', 2025),
    ('2025-11', 11, 'NOV', 'Novembro', 'Q4', 'S2', 2025),
    ('2025-12', 12, 'DEZ', 'Dezembro', 'Q4', 'S2', 2025);

-- -----------------------------------------------------------------------------
-- Tabela: dw.dim_unidade
-- Descrição: Dimensão de unidades de negócio
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.dim_unidade CASCADE;

CREATE TABLE dw.dim_unidade (
    unidade_key         SERIAL PRIMARY KEY,
    unidade             VARCHAR(100) NOT NULL UNIQUE,
    is_active           BOOLEAN DEFAULT TRUE,
    dw_loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dw.dim_unidade IS 'Dimensão de unidades de negócio';

CREATE INDEX idx_dim_unidade_nome ON dw.dim_unidade(unidade);

-- -----------------------------------------------------------------------------
-- Tabela: dw.dim_tipo_receita
-- Descrição: Dimensão de tipos de receita
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.dim_tipo_receita CASCADE;

CREATE TABLE dw.dim_tipo_receita (
    tipo_receita_key    SERIAL PRIMARY KEY,
    tipo_receita        VARCHAR(20) NOT NULL UNIQUE,  -- SALES, SERVICE
    descricao           VARCHAR(100),
    dw_loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dw.dim_tipo_receita IS 'Dimensão de tipos de receita';

-- Inserir dados estáticos
INSERT INTO dw.dim_tipo_receita (tipo_receita, descricao)
VALUES
    ('SALES', 'Vendas de Produtos'),
    ('SERVICE', 'Prestação de Serviços');

-- -----------------------------------------------------------------------------
-- Tabela: dw.dim_cenario
-- Descrição: Dimensão de cenários (Realizado vs Orçado)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.dim_cenario CASCADE;

CREATE TABLE dw.dim_cenario (
    cenario_key         SERIAL PRIMARY KEY,
    cenario             VARCHAR(20) NOT NULL UNIQUE,  -- Realizado, Orçado
    descricao           VARCHAR(100),
    dw_loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dw.dim_cenario IS 'Dimensão de cenários (Real vs Orçado)';

-- Inserir dados estáticos
INSERT INTO dw.dim_cenario (cenario, descricao)
VALUES
    ('Realizado', 'Valores efetivamente realizados'),
    ('Orçado', 'Valores planejados/orçados');

-- -----------------------------------------------------------------------------
-- Tabela: dw.dim_pacote
-- Descrição: Dimensão de pacotes de despesa
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.dim_pacote CASCADE;

CREATE TABLE dw.dim_pacote (
    pacote_key          SERIAL PRIMARY KEY,
    pacote              VARCHAR(100) NOT NULL UNIQUE,
    is_active           BOOLEAN DEFAULT TRUE,
    dw_loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dw.dim_pacote IS 'Dimensão de pacotes de despesas';

CREATE INDEX idx_dim_pacote_nome ON dw.dim_pacote(pacote);

-- -----------------------------------------------------------------------------
-- Tabela: dw.dim_linha_dre
-- Descrição: Dimensão de linhas da DRE
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.dim_linha_dre CASCADE;

CREATE TABLE dw.dim_linha_dre (
    linha_dre_key       SERIAL PRIMARY KEY,
    linha_dre           VARCHAR(100) NOT NULL UNIQUE,
    categoria           VARCHAR(50) NOT NULL,
    ordem               INTEGER NOT NULL,
    nivel               INTEGER NOT NULL,          -- 1=Total, 2=Subtotal, 3=Detalhe
    is_total            BOOLEAN DEFAULT FALSE,
    dw_loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dw.dim_linha_dre IS 'Dimensão de linhas da DRE com hierarquia';

CREATE INDEX idx_dim_linha_dre_ordem ON dw.dim_linha_dre(ordem);

-- =============================================================================
-- FATOS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Tabela: dw.fact_receita
-- Descrição: Fato de receitas por cenário/tipo/unidade/mês
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.fact_receita CASCADE;

CREATE TABLE dw.fact_receita (
    receita_key         SERIAL PRIMARY KEY,
    data_key            VARCHAR(10) NOT NULL REFERENCES dw.dim_calendario(data_key),
    cenario             VARCHAR(20) NOT NULL,
    tipo_receita        VARCHAR(20) NOT NULL,
    unidade             VARCHAR(100) NOT NULL,
    valor               DECIMAL(18,2) NOT NULL,
    dw_loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dw.fact_receita IS 'Fato de receitas';

CREATE INDEX idx_fact_receita_data ON dw.fact_receita(data_key);
CREATE INDEX idx_fact_receita_cenario ON dw.fact_receita(cenario);
CREATE INDEX idx_fact_receita_tipo ON dw.fact_receita(tipo_receita);

-- -----------------------------------------------------------------------------
-- Tabela: dw.fact_despesa
-- Descrição: Fato de despesas por cenário/pacote/unidade/mês
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.fact_despesa CASCADE;

CREATE TABLE dw.fact_despesa (
    despesa_key         SERIAL PRIMARY KEY,
    data_key            VARCHAR(10) NOT NULL REFERENCES dw.dim_calendario(data_key),
    cenario             VARCHAR(20) NOT NULL,
    unidade             VARCHAR(100) NOT NULL,
    pacote              VARCHAR(100) NOT NULL,
    conta               VARCHAR(255),
    valor               DECIMAL(18,2) NOT NULL,
    dw_loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dw.fact_despesa IS 'Fato de despesas';

CREATE INDEX idx_fact_despesa_data ON dw.fact_despesa(data_key);
CREATE INDEX idx_fact_despesa_cenario ON dw.fact_despesa(cenario);
CREATE INDEX idx_fact_despesa_pacote ON dw.fact_despesa(pacote);

-- -----------------------------------------------------------------------------
-- Tabela: dw.fact_dre
-- Descrição: Fato do modelo DRE por linha/mês
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.fact_dre CASCADE;

CREATE TABLE dw.fact_dre (
    dre_key             SERIAL PRIMARY KEY,
    data_key            VARCHAR(10) NOT NULL REFERENCES dw.dim_calendario(data_key),
    cenario             VARCHAR(20) NOT NULL DEFAULT 'Realizado',
    linha_dre           VARCHAR(100) NOT NULL,
    valor               DECIMAL(18,2) NOT NULL,
    dw_loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dw.fact_dre IS 'Fato do modelo DRE';

CREATE INDEX idx_fact_dre_data ON dw.fact_dre(data_key);
CREATE INDEX idx_fact_dre_linha ON dw.fact_dre(linha_dre);

-- -----------------------------------------------------------------------------
-- Tabela: dw.fact_aliquota
-- Descrição: Fato de alíquotas de imposto
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.fact_aliquota CASCADE;

CREATE TABLE dw.fact_aliquota (
    aliquota_key        SERIAL PRIMARY KEY,
    data_key            VARCHAR(10) NOT NULL REFERENCES dw.dim_calendario(data_key),
    tipo_imposto        VARCHAR(100) NOT NULL,
    aliquota            DECIMAL(10,6) NOT NULL,
    dw_loaded_at        TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dw.fact_aliquota IS 'Fato de alíquotas de imposto';

CREATE INDEX idx_fact_aliquota_data ON dw.fact_aliquota(data_key);
CREATE INDEX idx_fact_aliquota_tipo ON dw.fact_aliquota(tipo_imposto);
