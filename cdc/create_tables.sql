-- Tabela original para captura de mudanças
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(100),
    salary NUMERIC,
    start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Tabela para armazenar as mudanças capturadas (CDC)
CREATE TABLE IF NOT EXISTS employees_cdc (
    cdc_id SERIAL PRIMARY KEY,
    change_type VARCHAR(10),
    id INTEGER,
    name VARCHAR(100),
    department VARCHAR(100),
    salary NUMERIC,
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
