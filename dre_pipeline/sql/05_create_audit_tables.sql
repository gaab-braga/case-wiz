-- =============================================================================
-- DRE Analytics 2025
-- 05_create_audit_tables.sql
-- =============================================================================
-- Descrição: Tabelas de auditoria e controle de execução do ETL
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Tabela: dw.etl_run
-- Descrição: Registro de execuções do pipeline
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.etl_run CASCADE;

CREATE TABLE dw.etl_run (
    run_id              SERIAL PRIMARY KEY,
    pipeline_name       VARCHAR(100) DEFAULT 'dre_pipeline',
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ,
    status              VARCHAR(20) NOT NULL,      -- RUNNING, SUCCESS, FAILED
    duration_seconds    DECIMAL(10,2),
    total_rows_processed INTEGER DEFAULT 0,
    triggered_by        VARCHAR(50),               -- MANUAL, SCHEDULED, API
    error_message       TEXT
);

COMMENT ON TABLE dw.etl_run IS 'Registro de execuções do pipeline ETL';

CREATE INDEX idx_etl_run_status ON dw.etl_run(status);
CREATE INDEX idx_etl_run_started ON dw.etl_run(started_at);

-- -----------------------------------------------------------------------------
-- Tabela: dw.etl_step_log
-- Descrição: Log detalhado de cada step do pipeline
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.etl_step_log CASCADE;

CREATE TABLE dw.etl_step_log (
    log_id              SERIAL PRIMARY KEY,
    run_id              INTEGER REFERENCES dw.etl_run(run_id),
    step_name           VARCHAR(100) NOT NULL,
    step_order          INTEGER,
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    status              VARCHAR(20) NOT NULL,      -- RUNNING, SUCCESS, FAILED, SKIPPED
    rows_read           INTEGER DEFAULT 0,
    rows_written        INTEGER DEFAULT 0,
    rows_updated        INTEGER DEFAULT 0,
    rows_deleted        INTEGER DEFAULT 0,
    error_message       TEXT
);

COMMENT ON TABLE dw.etl_step_log IS 'Log detalhado de cada step do pipeline';

CREATE INDEX idx_etl_step_run ON dw.etl_step_log(run_id);
CREATE INDEX idx_etl_step_name ON dw.etl_step_log(step_name);

-- -----------------------------------------------------------------------------
-- Tabela: dw.data_quality_results
-- Descrição: Resultados das validações de qualidade de dados
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dw.data_quality_results CASCADE;

CREATE TABLE dw.data_quality_results (
    result_id           SERIAL PRIMARY KEY,
    run_id              INTEGER REFERENCES dw.etl_run(run_id),
    rule_id             INTEGER NOT NULL,
    rule_name           VARCHAR(100) NOT NULL,
    rule_description    VARCHAR(500),
    status              VARCHAR(10) NOT NULL,      -- PASS, FAIL, WARN
    expected_value      DECIMAL(18,2),
    actual_value        DECIMAL(18,2),
    difference_value    DECIMAL(18,2),
    difference_percent  DECIMAL(10,4),
    message             TEXT,
    checked_at          TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE dw.data_quality_results IS 'Resultados das validações de DQ';

CREATE INDEX idx_dq_results_run ON dw.data_quality_results(run_id);
CREATE INDEX idx_dq_results_status ON dw.data_quality_results(status);

-- -----------------------------------------------------------------------------
-- View: dw.v_etl_run_summary
-- Descrição: Resumo das últimas execuções
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW dw.v_etl_run_summary AS
SELECT 
    r.run_id,
    r.started_at,
    r.finished_at,
    r.status,
    r.duration_seconds,
    r.total_rows_processed,
    r.triggered_by,
    COUNT(DISTINCT s.log_id) as total_steps,
    SUM(CASE WHEN s.status = 'SUCCESS' THEN 1 ELSE 0 END) as steps_success,
    SUM(CASE WHEN s.status = 'FAILED' THEN 1 ELSE 0 END) as steps_failed,
    SUM(CASE WHEN q.status = 'PASS' THEN 1 ELSE 0 END) as dq_passed,
    SUM(CASE WHEN q.status = 'FAIL' THEN 1 ELSE 0 END) as dq_failed
FROM dw.etl_run r
LEFT JOIN dw.etl_step_log s ON r.run_id = s.run_id
LEFT JOIN dw.data_quality_results q ON r.run_id = q.run_id
GROUP BY r.run_id, r.started_at, r.finished_at, r.status, 
         r.duration_seconds, r.total_rows_processed, r.triggered_by
ORDER BY r.started_at DESC;

COMMENT ON VIEW dw.v_etl_run_summary IS 'Resumo das execuções do ETL';
