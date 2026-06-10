"""
Module : quality.py
===================
Contrôle qualité des données météo transformées et logique de branchement
conditionnel (BranchPythonOperator).

Règles de qualité appliquées :
  1. Présence des champs obligatoires
  2. Température entre -50°C et +60°C
  3. Vitesse du vent entre 0 et 300 km/h
  4. Direction du vent entre 0° et 360°
  5. observation_time non vide

La fonction ``branch_on_quality_result`` détermine la branche suivante :
  - ``"load_postgres"``  si toutes les règles sont satisfaites
  - ``"log_anomaly"``    si au moins une anomalie est détectée
"""

import logging
from typing import Any, Dict, List, Tuple

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Règles de qualité (facilement extensibles)
# ──────────────────────────────────────────────────────────────────────────────
NUMERIC_RANGE_RULES: Dict[str, Dict[str, Any]] = {
    "temperature_2m": {
        "min": -50.0,
        "max": 60.0,
        "description": "Température entre -50°C et +60°C",
    },
    "windspeed_10m": {
        "min": 0.0,
        "max": 300.0,
        "description": "Vitesse du vent entre 0 et 300 km/h",
    },
    "winddirection_10m": {
        "min": 0.0,
        "max": 360.0,
        "description": "Direction du vent entre 0° et 360°",
    },
}

REQUIRED_FIELDS: List[str] = [
    "city_name",
    "execution_date",
    "temperature_2m",
    "windspeed_10m",
    "observation_time",
]


# ──────────────────────────────────────────────────────────────────────────────
# Vérification d'un enregistrement unique
# ──────────────────────────────────────────────────────────────────────────────
def _check_record(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Applique toutes les règles de qualité à un enregistrement.

    Retourne :
      - ``(True, [])``          si l'enregistrement est valide
      - ``(False, [messages])`` si des anomalies ont été trouvées
    """
    city = record.get("city_name", "ville inconnue")
    anomalies: List[str] = []

    # Règle 1 – Champs obligatoires
    for field in REQUIRED_FIELDS:
        value = record.get(field)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            anomalies.append(
                f"[{city}] Champ obligatoire manquant ou vide : '{field}'"
            )

    # Règle 2 – Plages de valeurs numériques
    for field, rules in NUMERIC_RANGE_RULES.items():
        if field not in record or record[field] is None:
            continue  # Déjà couvert par règle 1 si le champ est obligatoire
        try:
            value = float(record[field])
            if not (rules["min"] <= value <= rules["max"]):
                anomalies.append(
                    f"[{city}] {field}={value} hors plage "
                    f"[{rules['min']}, {rules['max']}] — {rules['description']}"
                )
        except (TypeError, ValueError):
            anomalies.append(
                f"[{city}] {field} n'est pas un nombre valide : {record[field]!r}"
            )

    is_valid = len(anomalies) == 0
    return is_valid, anomalies


# ──────────────────────────────────────────────────────────────────────────────
# Callable : check_data_quality (PythonOperator)
# ──────────────────────────────────────────────────────────────────────────────
def check_data_quality(**context: Any) -> None:
    """
    Tâche Airflow : contrôle qualité des enregistrements transformés.

    Étapes :
      1. Pull XCom ``transformed_records`` depuis ``transform_data``
      2. Application de toutes les règles de qualité sur chaque enregistrement
      3. Synthèse du résultat (quality_passed / anomalies)
      4. Push XCom :
         - ``quality_passed``     : bool – True si aucune anomalie
         - ``quality_anomalies``  : liste des messages d'anomalie
         - ``valid_records_count``: nombre d'enregistrements valides
    """
    ti = context["ti"]

    transformed_records: List[Dict[str, Any]] = ti.xcom_pull(
        task_ids="transform_data", key="transformed_records"
    ) or []

    if not transformed_records:
        log.error(
            "Aucun enregistrement transformé disponible. Contrôle qualité impossible."
        )
        ti.xcom_push(key="quality_passed", value=False)
        ti.xcom_push(
            key="quality_anomalies",
            value=["Aucune donnée à valider – tâche transform_data sans résultat"],
        )
        ti.xcom_push(key="valid_records_count", value=0)
        return

    log.info(
        "=== [check_data_quality] Début | %d enregistrements à vérifier ===",
        len(transformed_records),
    )

    all_anomalies: List[str] = []
    valid_count = 0

    for record in transformed_records:
        city = record.get("city_name", "?")
        is_valid, anomalies = _check_record(record)

        if is_valid:
            valid_count += 1
            log.info("Qualité OK  ✓  %s (temp=%.1f°C, vent=%.1f km/h)",
                     city,
                     record.get("temperature_2m", float("nan")),
                     record.get("windspeed_10m", float("nan")))
        else:
            all_anomalies.extend(anomalies)
            for msg in anomalies:
                log.warning("Anomalie QC ✗  %s", msg)

    quality_passed = len(all_anomalies) == 0

    if quality_passed:
        log.info(
            "=== [check_data_quality] RÉUSSI – %d/%d enregistrements valides ===",
            valid_count,
            len(transformed_records),
        )
    else:
        log.warning(
            "=== [check_data_quality] ÉCHOUÉ – %d anomalie(s) sur %d enregistrement(s) ===",
            len(all_anomalies),
            len(transformed_records),
        )

    ti.xcom_push(key="quality_passed", value=quality_passed)
    ti.xcom_push(key="quality_anomalies", value=all_anomalies)
    ti.xcom_push(key="valid_records_count", value=valid_count)


# ──────────────────────────────────────────────────────────────────────────────
# Callable : branch_on_quality_result (BranchPythonOperator)
# ──────────────────────────────────────────────────────────────────────────────
def branch_on_quality_result(**context: Any) -> str:
    """
    Fonction de branchement conditionnel.

    Lit ``quality_passed`` depuis XCom et retourne :
      - ``"load_postgres"`` si la qualité est validée
      - ``"log_anomaly"``   si des anomalies ont été détectées

    Cette valeur de retour est utilisée par le BranchPythonOperator pour
    déterminer quelle(s) tâche(s) exécuter et lesquelles ignorer (SKIPPED).
    """
    ti = context["ti"]

    quality_passed: bool = ti.xcom_pull(
        task_ids="check_data_quality", key="quality_passed"
    )

    if quality_passed:
        log.info(
            "Branchement → 'load_postgres' (contrôle qualité validé). "
            "Les données seront chargées dans PostgreSQL."
        )
        return "load_postgres"

    anomalies: List[str] = ti.xcom_pull(
        task_ids="check_data_quality", key="quality_anomalies"
    ) or []

    log.warning(
        "Branchement → 'log_anomaly' (anomalie(s) détectée(s): %d). "
        "Le chargement PostgreSQL est BLOQUÉ.",
        len(anomalies),
    )
    return "log_anomaly"
