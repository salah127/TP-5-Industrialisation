"""
Module : anomaly.py
===================
Traçabilité et journalisation des anomalies de contrôle qualité.

Responsabilités :
  - Récupérer les anomalies depuis XCom (tâche check_data_quality)
  - Les logguer de façon visible dans les logs Airflow
  - Les persister dans la table PostgreSQL ``ingestion_log``
  - NE PAS charger les données dans ``weather_current`` (chargement bloqué)
"""

import logging
from typing import Any, Dict, List

from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
POSTGRES_CONN_ID = "postgres_weather"

INSERT_ANOMALY_LOG_SQL = """
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
def log_anomaly(**context: Any) -> None:
    """
    Tâche Airflow : journalisation et traçabilité des anomalies qualité.

    Cette tâche est exécutée UNIQUEMENT quand le branchement conditionnel
    détermine que les données ne satisfont pas le contrôle qualité.
    Elle ne charge JAMAIS de données dans ``weather_current``.

    Étapes :
      1. Pull XCom ``quality_anomalies`` depuis ``check_data_quality``
      2. Log détaillé dans les logs Airflow (visible dans l'interface)
      3. Persistance dans la table ``ingestion_log`` (statut = quality_failure)
      4. Log d'un message de clôture expliquant que le chargement est bloqué
    """
    ti = context["ti"]
    dag_run = context["dag_run"]
    execution_date: str = context["ds"]

    anomalies: List[str] = (
        ti.xcom_pull(task_ids="check_data_quality", key="quality_anomalies") or []
    )
    transformed_records: List[Dict[str, Any]] = (
        ti.xcom_pull(task_ids="transform_data", key="transformed_records") or []
    )

    # ── Log structuré dans les logs Airflow ───────────────────────────────
    separator = "═" * 65
    log.warning(separator)
    log.warning("ANOMALIE QUALITÉ DÉTECTÉE")
    log.warning("  DAG          : %s", dag_run.dag_id)
    log.warning("  Run ID       : %s", dag_run.run_id)
    log.warning("  Date         : %s", execution_date)
    log.warning("  Villes       : %d enregistrement(s) analysé(s)", len(transformed_records))
    log.warning("  Anomalies    : %d détectée(s)", len(anomalies))
    log.warning(separator)

    for idx, anomaly in enumerate(anomalies, start=1):
        log.warning("  [%02d] %s", idx, anomaly)

    log.warning(separator)
    log.warning(
        "ACTION : chargement des données dans 'weather_current' BLOQUÉ. "
        "Corriger les anomalies et relancer le DAG."
    )
    log.warning(separator)

    # ── Persistance dans PostgreSQL ────────────────────────────────────────
    anomaly_details = "; ".join(anomalies) if anomalies else "Anomalie non spécifiée"
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        log_params: Dict[str, Any] = {
            "dag_id": dag_run.dag_id,
            "run_id": dag_run.run_id,
            "execution_date": execution_date,
            "status": "quality_failure",
            "cities_processed": len(transformed_records),
            "quality_passed": False,
            "anomaly_details": anomaly_details,
        }
        pg_hook.run(INSERT_ANOMALY_LOG_SQL, parameters=log_params)
        log.info(
            "Anomalie persistée dans 'ingestion_log' (statut=quality_failure)."
        )
    except Exception as exc:  # noqa: BLE001
        log.error(
            "Impossible de persister l'anomalie dans PostgreSQL : %s. "
            "L'anomalie est tracée uniquement dans les logs Airflow.",
            exc,
        )
