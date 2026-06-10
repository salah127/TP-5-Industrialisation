-- ============================================================
-- Script : upsert_weather.sql
-- Description : Insertion idempotente dans weather_current.
--
-- Stratégie d'idempotence :
--   La contrainte UNIQUE (city_name, execution_date) garantit
--   qu'une relance du DAG pour la même date ne crée pas de
--   doublon. En cas de conflit, l'enregistrement existant est
--   MIS À JOUR avec les nouvelles valeurs (DO UPDATE SET).
--
-- Usage (Python / psycopg2) :
--   cursor.execute(UPSERT_SQL, record_dict)
--
-- Paramètres attendus (dict) :
--   city_name, execution_date, temperature_2m, windspeed_10m,
--   winddirection_10m, weathercode, is_day, observation_time,
--   latitude, longitude
-- ============================================================

INSERT INTO weather_current (
    city_name,
    execution_date,
    temperature_2m,
    windspeed_10m,
    winddirection_10m,
    weathercode,
    is_day,
    observation_time,
    latitude,
    longitude,
    quality_status,
    updated_at
)
VALUES (
    %(city_name)s,
    %(execution_date)s,
    %(temperature_2m)s,
    %(windspeed_10m)s,
    %(winddirection_10m)s,
    %(weathercode)s,
    %(is_day)s,
    %(observation_time)s,
    %(latitude)s,
    %(longitude)s,
    'valid',
    NOW()
)
ON CONFLICT (city_name, execution_date)
DO UPDATE SET
    temperature_2m    = EXCLUDED.temperature_2m,
    windspeed_10m     = EXCLUDED.windspeed_10m,
    winddirection_10m = EXCLUDED.winddirection_10m,
    weathercode       = EXCLUDED.weathercode,
    is_day            = EXCLUDED.is_day,
    observation_time  = EXCLUDED.observation_time,
    quality_status    = 'valid',
    updated_at        = NOW();

-- ============================================================
-- Requête de vérification post-chargement
-- ============================================================
-- SELECT city_name, execution_date, temperature_2m, windspeed_10m,
--        quality_status, updated_at
-- FROM   weather_current
-- ORDER BY execution_date DESC, city_name;
