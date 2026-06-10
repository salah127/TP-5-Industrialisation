"""
Module : transformer.py
=======================
Transformation des données météo brutes (JSON archivé) en enregistrements
structurés prêts pour le contrôle qualité et le chargement PostgreSQL.

Responsabilités :
  - Lire les fichiers JSON archivés pointés par les XCom de la tâche précédente
  - Extraire et caster les champs utiles (température, vent, direction, code météo…)
  - Supporter l'injection d'anomalie via la variable Airflow
    ``open_meteo_simulate_anomaly`` (cas de démonstration académique)
  - Pousser les enregistrements transformés via XCom
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

from airflow.models import Variable

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers privés
# ──────────────────────────────────────────────────────────────────────────────
def _is_anomaly_simulation_active() -> bool:
    """
    Vérifie si la simulation d'anomalie est activée via la variable Airflow
    ``open_meteo_simulate_anomaly``.

    Pour démontrer le cas d'anomalie qualité, créer la variable dans
    Admin > Variables avec la valeur ``true``.
    """
    try:
        value = Variable.get("open_meteo_simulate_anomaly", default_var="false")
        active = value.strip().lower() == "true"
        if active:
            log.warning(
                "⚠ SIMULATION D'ANOMALIE ACTIVÉE (variable 'open_meteo_simulate_anomaly'=true). "
                "Les données de la première ville seront corrompues volontairement."
            )
        return active
    except Exception:  # noqa: BLE001
        return False


def _parse_city_record(
    raw_data: Dict[str, Any],
    execution_date: str,
    inject_anomaly: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Transforme une réponse API brute en enregistrement structuré.

    Retourne None si les données sont inexploitables (champ manquant critique).

    Paramètre ``inject_anomaly`` : si True, remplace la température par une
    valeur hors plage (999°C) pour déclencher le contrôle qualité.
    """
    city_name: str = raw_data.get("_city_name", "Unknown")
    fetched_at: str = raw_data.get("_fetched_at", "")

    current_weather = raw_data.get("current_weather")
    if not current_weather:
        log.warning(
            "Champ 'current_weather' absent dans les données de '%s'. Enregistrement ignoré.",
            city_name,
        )
        return None

    try:
        temperature = float(current_weather.get("temperature", 0.0))
        windspeed = float(current_weather.get("windspeed", 0.0))
        winddirection = float(current_weather.get("winddirection", 0.0))
        weathercode = int(current_weather.get("weathercode", 0))
        is_day = bool(int(current_weather.get("is_day", 0)))
        observation_time: str = current_weather.get("time", "")

        # ── Injection d'anomalie pour démonstration ────────────────────────
        if inject_anomaly:
            original_temp = temperature
            temperature = 999.0  # Hors plage autorisée [-50, 60]
            log.warning(
                "INJECTION ANOMALIE pour '%s': température forcée de %.1f°C à 999.0°C",
                city_name,
                original_temp,
            )

        record: Dict[str, Any] = {
            "city_name": city_name,
            "execution_date": execution_date,
            "temperature_2m": temperature,
            "windspeed_10m": windspeed,
            "winddirection_10m": winddirection,
            "weathercode": weathercode,
            "is_day": is_day,
            "observation_time": observation_time,
            "latitude": float(raw_data.get("latitude", 0.0)),
            "longitude": float(raw_data.get("longitude", 0.0)),
            "fetched_at": fetched_at,
        }

        log.info(
            "Transformation OK pour '%s' : temp=%.1f°C | vent=%.1f km/h | "
            "direction=%.0f° | code=%d | is_day=%s",
            city_name,
            temperature,
            windspeed,
            winddirection,
            weathercode,
            is_day,
        )
        return record

    except (KeyError, ValueError, TypeError) as exc:
        log.error(
            "Erreur de conversion des données pour '%s': %s", city_name, exc
        )
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Callable principal (PythonOperator)
# ──────────────────────────────────────────────────────────────────────────────
def transform_data(**context: Any) -> None:
    """
    Tâche Airflow : transformation des données brutes archivées.

    Étapes :
      1. Pull XCom ``archive_paths`` depuis la tâche ``extract_and_archive``
      2. Lecture de chaque fichier JSON
      3. Extraction/cast des champs utiles via ``_parse_city_record``
      4. Support de la simulation d'anomalie qualité
      5. Push XCom :
         - ``transformed_records`` : liste de dicts structurés
         - ``transform_errors``    : villes en échec de transformation

    Lève RuntimeError si aucun enregistrement n'a pu être transformé.
    """
    ti = context["ti"]
    execution_date: str = context["ds"]

    archive_paths: Dict[str, str] = ti.xcom_pull(
        task_ids="extract_and_archive", key="archive_paths"
    )

    if not archive_paths:
        raise ValueError(
            "XCom 'archive_paths' introuvable. La tâche 'extract_and_archive' "
            "n'a peut-être pas abouti."
        )

    simulate_anomaly = _is_anomaly_simulation_active()

    log.info(
        "=== [transform_data] Début | date=%s | %d fichiers à transformer ===",
        execution_date,
        len(archive_paths),
    )

    transformed_records: List[Dict[str, Any]] = []
    transform_errors: List[str] = []

    # La première ville reçoit l'injection d'anomalie si la simulation est active
    first_city = True

    for city_name, file_path in archive_paths.items():
        inject = simulate_anomaly and first_city
        first_city = False

        try:
            if not os.path.exists(file_path):
                log.error(
                    "Fichier archive introuvable pour '%s': %s", city_name, file_path
                )
                transform_errors.append(city_name)
                continue

            with open(file_path, "r", encoding="utf-8") as fh:
                raw_data: Dict[str, Any] = json.load(fh)

            record = _parse_city_record(raw_data, execution_date, inject_anomaly=inject)

            if record is not None:
                transformed_records.append(record)
            else:
                log.warning(
                    "Aucun enregistrement produit pour '%s'.", city_name
                )
                transform_errors.append(city_name)

        except json.JSONDecodeError as exc:
            log.error("JSON invalide pour '%s': %s", city_name, exc)
            transform_errors.append(city_name)
        except OSError as exc:
            log.error("Erreur lecture fichier pour '%s': %s", city_name, exc)
            transform_errors.append(city_name)
        except Exception as exc:  # noqa: BLE001
            log.error("Erreur inattendue pour '%s': %s", city_name, exc)
            transform_errors.append(city_name)

    if not transformed_records:
        raise RuntimeError(
            f"Transformation complètement échouée. Aucun enregistrement produit. "
            f"Erreurs : {transform_errors}"
        )

    if transform_errors:
        log.warning(
            "Transformation partielle : %d OK, %d échecs (%s)",
            len(transformed_records),
            len(transform_errors),
            transform_errors,
        )

    log.info(
        "=== [transform_data] Terminé : %d enregistrements produits ===",
        len(transformed_records),
    )

    ti.xcom_push(key="transformed_records", value=transformed_records)
    ti.xcom_push(key="transform_errors", value=transform_errors)
