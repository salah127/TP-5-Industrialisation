-- ============================================================
-- Script : create_tables.sql
-- Description : Création des tables PostgreSQL pour le pipeline
--               Open-Meteo. Idempotent (IF NOT EXISTS).
-- ============================================================

-- ------------------------------------------------------------
-- Table principale : données météo par ville et par jour
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_current (
    id                SERIAL PRIMARY KEY,
    city_name         VARCHAR(100)   NOT NULL,
    execution_date    DATE           NOT NULL,
    temperature_2m    NUMERIC(7, 2),
    windspeed_10m     NUMERIC(7, 2),
    winddirection_10m NUMERIC(7, 2),
    weathercode       INTEGER,
    is_day            BOOLEAN,
    observation_time  VARCHAR(30),
    latitude          NUMERIC(10, 6),
    longitude         NUMERIC(10, 6),
    quality_status    VARCHAR(20)    NOT NULL DEFAULT 'valid',
    ingested_at       TIMESTAMP      NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMP      NOT NULL DEFAULT NOW(),

    -- Contrainte d'unicité pour l'idempotence :
    -- ON CONFLICT (city_name, execution_date) DO UPDATE
    CONSTRAINT uq_weather_city_date UNIQUE (city_name, execution_date)
);

COMMENT ON TABLE weather_current
    IS 'Données météo actuelles par ville et par date d''exécution du DAG.';
COMMENT ON COLUMN weather_current.city_name
    IS 'Nom de la ville (ex. Paris, Lyon, Marseille).';
COMMENT ON COLUMN weather_current.execution_date
    IS 'Date d''exécution du DAG (contexte ds – YYYY-MM-DD).';
COMMENT ON COLUMN weather_current.temperature_2m
    IS 'Température à 2 m du sol en °C.';
COMMENT ON COLUMN weather_current.windspeed_10m
    IS 'Vitesse du vent à 10 m en km/h.';
COMMENT ON COLUMN weather_current.winddirection_10m
    IS 'Direction du vent à 10 m en degrés (0-360).';
COMMENT ON COLUMN weather_current.weathercode
    IS 'Code météo WMO (0 = ciel dégagé, 95 = orage, …).';
COMMENT ON COLUMN weather_current.quality_status
    IS 'Statut du contrôle qualité : valid.';
COMMENT ON COLUMN weather_current.ingested_at
    IS 'Timestamp du premier chargement.';
COMMENT ON COLUMN weather_current.updated_at
    IS 'Timestamp de la dernière mise à jour (relance idempotente).';

-- ------------------------------------------------------------
-- Table de traçabilité : journal des exécutions du pipeline
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_log (
    id               SERIAL PRIMARY KEY,
    dag_id           VARCHAR(200),
    run_id           VARCHAR(500),
    execution_date   DATE,
    status           VARCHAR(50),
    cities_processed INTEGER      DEFAULT 0,
    quality_passed   BOOLEAN,
    anomaly_details  TEXT,
    created_at       TIMESTAMP    NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE ingestion_log
    IS 'Journal de traçabilité de toutes les exécutions du pipeline Open-Meteo.';
COMMENT ON COLUMN ingestion_log.dag_id
    IS 'Identifiant du DAG Airflow.';
COMMENT ON COLUMN ingestion_log.run_id
    IS 'Identifiant unique du run Airflow.';
COMMENT ON COLUMN ingestion_log.status
    IS 'Statut : success | partial | quality_failure | error.';
COMMENT ON COLUMN ingestion_log.quality_passed
    IS 'True si le contrôle qualité a été validé, False sinon.';
COMMENT ON COLUMN ingestion_log.anomaly_details
    IS 'Description textuelle des anomalies détectées (NULL si succès).';

-- ------------------------------------------------------------
-- Index pour améliorer les performances de requête
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_weather_current_city
    ON weather_current (city_name);

CREATE INDEX IF NOT EXISTS idx_weather_current_date
    ON weather_current (execution_date);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_date
    ON ingestion_log (execution_date);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_status
    ON ingestion_log (status);
