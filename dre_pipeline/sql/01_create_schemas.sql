-- =============================================================================
-- DRE Analytics 2025
-- 01_create_schemas.sql
-- =============================================================================
-- Descrição: Criação dos schemas do Data Warehouse (Arquitetura Medallion)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Schema RAW (Bronze): Dados brutos como chegam
-- Contém dados brutos vindos do Excel sem transformação
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw;

COMMENT ON SCHEMA raw IS 'Camada Bronze - Dados brutos extraídos do Excel';

-- -----------------------------------------------------------------------------
-- Schema STG (Silver): Dados limpos e normalizados
-- Contém dados após limpeza, padronização e validação
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS stg;

COMMENT ON SCHEMA stg IS 'Camada Silver - Dados limpos, padronizados e validados';

-- -----------------------------------------------------------------------------
-- Schema DW (Gold): Star Schema para análise
-- Contém dimensões e fatos otimizados para Power BI
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS dw;

COMMENT ON SCHEMA dw IS 'Camada Gold - Star Schema com dimensões e fatos para BI';

-- -----------------------------------------------------------------------------
-- Verificação
-- -----------------------------------------------------------------------------
SELECT 
    schema_name,
    schema_owner
FROM information_schema.schemata
WHERE schema_name IN ('raw', 'stg', 'dw')
ORDER BY schema_name;
