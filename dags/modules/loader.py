"""
Module : loader.py
==================
Chargement des données météo transformées et validées dans PostgreSQL.

Responsabilités :
  - Utiliser le Hook Airflow ``PostgresHook`` (connexion ``postgres_weather``)
  - Insérer les enregistrements avec ``INSERT … ON CONFLICT DO UPDATE``
    → garantit l'idempotence : une relance ne crée pas de doublons
  - Enregistrer une ligne de traçabilité dans la table ``ingestion_log``
"""

import logging
from typing import Any, Dict, List

from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
POSTGRES_CONN_ID = "postgres_weather"

# ── SQL idempotent (INSERT … ON CONFLICT DO UPDATE) ───────────────────────────
UPSERT_WEATHER_SQL = """
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
"""

# ── SQL de traçabilité ────────────────────────────────────────────────────────
INSERT_INGESTION_LOG_SQL = """
INSERT INTO ingestion_log (
    dag_id,
    run_id,
    execution_date,
    status,
    cities_processed,
    quality_passed,
    anomaly_details
)
VALUES (
    %(dag_id)s,
    %(run_id)s,
    %(execution_date)s,
    %(status)s,
    %(cities_processed)s,
    %(quality_passed)s,
    %(anomaly_details)s
);
"""


# ──────────────────────────────────────────────────────────────────────────────
# Callable principal (PythonOperator)
# ──────────────────────────────────────────────────────────────────────────────
def load_to_postgres(**context: Any) -> None:
    """
    Tâche Airflow : chargement idempotent dans PostgreSQL.

    Étapes :
      1. Pull XCom ``transformed_records`` depuis ``transform_data``
      2. Connexion à PostgreSQL via ``PostgresHook`` (conn_id=``postgres_weather``)
      3. INSERT … ON CONFLICT DO UPDATE pour chaque ville
         → idempotence garantie : une relance met à jour sans dupliquer
      4. Insertion d'une ligne de traçabilité dans ``ingestion_log``

    Lève RuntimeError si au moins un enregistrement n'a pas pu être chargé.
    """
    ti = context["ti"]
    dag_run = context["dag_run"]
    execution_date: str = context["ds"]

    transformed_records: List[Dict[str, Any]] = ti.xcom_pull(
        task_ids="transform_data", key="transformed_records"
    ) or []

    if not transformed_records:
        raise ValueError(
            "Aucun enregistrement transformé disponible pour le chargement."
        )

    log.info(
        "=== [load_to_postgres] Début | date=%s | %d enregistrements ===",
        execution_date,
        len(transformed_records),
    )

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    loaded_count = 0
    load_errors: List[str] = []

    for record in transformed_records:
        city_name: str = record.get("city_name", "?")
        try:
            pg_hook.run(UPSERT_WEATHER_SQL, parameters=record)
            loaded_count += 1
            log.info(
                "Chargement OK pour '%s' (date=%s, temp=%.1f°C) – "
                "INSERT OR UPDATE appliqué.",
                city_name,
                execution_date,
                record.get("temperature_2m", float("nan")),
            )
        except Exception as exc:  # noqa: BLE001
            log.error("Erreur de chargement pour '%s': %s", city_name, exc)
            load_errors.append(city_name)

    # ── Traçabilité dans ingestion_log ─────────────────────────────────────
    log_status = "success" if not load_errors else "partial"
    try:
        log_params: Dict[str, Any] = {
            "dag_id": dag_run.dag_id,
            "run_id": dag_run.run_id,
            "execution_date": execution_date,
            "status": log_status,
            "cities_processed": loaded_count,
            "quality_passed": True,
            "anomaly_details": str(load_errors) if load_errors else None,
        }
        pg_hook.run(INSERT_INGESTION_LOG_SQL, parameters=log_params)
        log.info(
            "Traçabilité enregistrée dans 'ingestion_log' (statut=%s).", log_status
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Impossible d'insérer dans 'ingestion_log': %s. "
            "Le chargement principal n'est pas affecté.",
            exc,
        )

    if load_errors:
        raise RuntimeError(
            f"Erreurs de chargement PostgreSQL pour les villes : {load_errors}"
        )

    log.info(
        "=== [load_to_postgres] Terminé : %d enregistrement(s) chargé(s) avec succès ===",
        loaded_count,
    )
