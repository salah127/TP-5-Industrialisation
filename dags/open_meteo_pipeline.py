"""
DAG : open_meteo_pipeline
=========================
Pipeline de collecte, transformation, contrôle qualité et chargement
des données météo Open-Meteo dans PostgreSQL.

Flux :
    extract_and_archive
        → transform_data
            → check_data_quality
                → branch_on_quality
                    → load_postgres   (si qualité OK)
                    → log_anomaly     (si anomalie détectée)
                        → end
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from modules.anomaly import log_anomaly
from modules.extractor import extract_and_archive
from modules.loader import load_to_postgres
from modules.quality import branch_on_quality_result, check_data_quality
from modules.transformer import transform_data

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Arguments par défaut (robustesse : retries, timeout, retry_delay)
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ──────────────────────────────────────────────────────────────────────────────
# Définition du DAG
# ──────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="open_meteo_pipeline",
    default_args=DEFAULT_ARGS,
    description=(
        "Pipeline météo Open-Meteo : extraction API, archivage JSON, "
        "transformation, contrôle qualité, chargement idempotent PostgreSQL."
    ),
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "open-meteo", "postgresql", "tp5"],
    doc_md="""
## Pipeline Open-Meteo – TP5

### Description
Ce DAG orchestre la collecte de données météo depuis l'API publique Open-Meteo
pour plusieurs villes configurables, leur transformation, leur contrôle qualité
et leur chargement dans une base PostgreSQL.

### Flux des tâches
```
extract_and_archive → transform_data → check_data_quality → branch_on_quality
    ├─ [qualité OK]  → load_postgres  → end
    └─ [anomalie]    → log_anomaly    → end
```

### Variables Airflow requises
| Variable | Description | Exemple |
|---|---|---|
| `open_meteo_cities` | JSON des villes à traiter | `[{"name":"Paris","latitude":48.8566,"longitude":2.3522}]` |
| `open_meteo_archive_path` | Répertoire d'archivage | `/opt/airflow/data/archive` |
| `open_meteo_simulate_anomaly` | Active la simulation d'anomalie | `true` |

### Connexions Airflow requises
| Connexion | Type | Description |
|---|---|---|
| `postgres_weather` | Postgres | Base PostgreSQL cible |
""",
) as dag:

    # ── 1. Extraction & Archivage ──────────────────────────────────────────────
    task_extract = PythonOperator(
        task_id="extract_and_archive",
        python_callable=extract_and_archive,
        doc_md="Appelle l'API Open-Meteo pour chaque ville et archive les réponses JSON brutes.",
    )

    # ── 2. Transformation ──────────────────────────────────────────────────────
    task_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        doc_md="Lit les archives JSON brutes et produit des enregistrements structurés.",
    )

    # ── 3. Contrôle Qualité ────────────────────────────────────────────────────
    task_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
        doc_md="Vérifie les règles de qualité (plages de valeurs, champs obligatoires).",
    )

    # ── 4. Branchement Conditionnel ────────────────────────────────────────────
    task_branch = BranchPythonOperator(
        task_id="branch_on_quality",
        python_callable=branch_on_quality_result,
        doc_md="Route vers load_postgres si qualité OK, sinon vers log_anomaly.",
    )

    # ── 5a. Chargement PostgreSQL ──────────────────────────────────────────────
    task_load = PythonOperator(
        task_id="load_postgres",
        python_callable=load_to_postgres,
        doc_md="Insert idempotent (ON CONFLICT DO UPDATE) dans weather_current.",
    )

    # ── 5b. Log Anomalie ───────────────────────────────────────────────────────
    task_anomaly = PythonOperator(
        task_id="log_anomaly",
        python_callable=log_anomaly,
        doc_md="Trace l'anomalie qualité dans les logs et dans ingestion_log. Ne charge PAS les données.",
    )

    # ── 6. Fin ─────────────────────────────────────────────────────────────────
    task_end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Tâche terminale – déclenche quelle que soit la branche empruntée.",
    )

    # ── Dépendances ────────────────────────────────────────────────────────────
    task_extract >> task_transform >> task_quality >> task_branch
    task_branch >> [task_load, task_anomaly]
    task_load >> task_end
    task_anomaly >> task_end
