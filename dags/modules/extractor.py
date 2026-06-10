"""
Module : extractor.py
=====================
Extraction des données météo depuis l'API Open-Meteo et archivage JSON brut.

Responsabilités :
  - Lire la liste des villes depuis la variable Airflow `open_meteo_cities`
  - Appeler l'endpoint /v1/forecast pour chaque ville
  - Sauvegarder la réponse JSON brute dans un répertoire d'archive daté
  - Garantir l'idempotence : si l'archive existe déjà, ne pas ré-appeler l'API
  - Pousser les chemins d'archive via XCom pour les tâches aval
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from airflow.models import Variable

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────────────────────────────────────
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
DEFAULT_ARCHIVE_BASE_PATH = "/opt/airflow/data/archive"
REQUEST_TIMEOUT_SECONDS = 30

DEFAULT_CITIES: List[Dict[str, Any]] = [
    {"name": "Paris",     "latitude": 48.8566, "longitude": 2.3522},
    {"name": "Lyon",      "latitude": 45.7640, "longitude": 4.8357},
    {"name": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers privés
# ──────────────────────────────────────────────────────────────────────────────
def _get_cities() -> List[Dict[str, Any]]:
    """
    Récupère la liste des villes depuis la variable Airflow `open_meteo_cities`.
    Retourne les villes par défaut si la variable est absente ou invalide.
    """
    try:
        cities_json = Variable.get("open_meteo_cities", default_var=None)
        if cities_json:
            cities = json.loads(cities_json)
            log.info(
                "Villes chargées depuis la variable Airflow: %s",
                [c["name"] for c in cities],
            )
            return cities
    except json.JSONDecodeError as exc:
        log.warning(
            "Variable 'open_meteo_cities' invalide (JSON malformé): %s. "
            "Utilisation des villes par défaut.",
            exc,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "Impossible de charger 'open_meteo_cities': %s. Villes par défaut utilisées.",
            exc,
        )

    log.info(
        "Utilisation des villes par défaut: %s",
        [c["name"] for c in DEFAULT_CITIES],
    )
    return DEFAULT_CITIES


def _get_archive_base_path() -> str:
    """Récupère le chemin d'archivage depuis la variable Airflow `open_meteo_archive_path`."""
    try:
        return Variable.get(
            "open_meteo_archive_path", default_var=DEFAULT_ARCHIVE_BASE_PATH
        )
    except Exception:  # noqa: BLE001
        return DEFAULT_ARCHIVE_BASE_PATH


def _fetch_weather(city: Dict[str, Any]) -> Dict[str, Any]:
    """
    Appelle l'API Open-Meteo pour une ville et retourne le JSON enrichi.

    Paramètres API :
      - current_weather=true   : données météo actuelles
      - hourly=...             : séries horaires du jour (température, humidité, vent)
      - forecast_days=1        : uniquement le jour en cours
      - timezone=Europe/Paris  : fuseau horaire local
    """
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current_weather": "true",
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m,precipitation",
        "forecast_days": 1,
        "timezone": "Europe/Paris",
    }

    log.info(
        "Appel API Open-Meteo pour '%s' (lat=%.4f, lon=%.4f)",
        city["name"],
        city["latitude"],
        city["longitude"],
    )

    response = requests.get(
        OPEN_METEO_BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS
    )
    response.raise_for_status()

    data: Dict[str, Any] = response.json()

    # Métadonnées d'enrichissement
    data["_city_name"] = city["name"]
    data["_fetched_at"] = datetime.utcnow().isoformat()

    current = data.get("current_weather", {})
    log.info(
        "Données reçues pour '%s' : température=%.1f°C, vent=%.1f km/h, code=%s",
        city["name"],
        current.get("temperature", float("nan")),
        current.get("windspeed", float("nan")),
        current.get("weathercode", "?"),
    )
    return data


def _archive_data(
    data: Dict[str, Any],
    archive_dir: str,
    city_name: str,
    execution_date: str,
) -> str:
    """
    Enregistre les données brutes en JSON dans le répertoire d'archive.

    Idempotence : si le fichier existe déjà pour cette ville et cette date,
    l'archivage est ignoré (pas de ré-écriture).

    Retourne le chemin absolu du fichier archivé.
    """
    os.makedirs(archive_dir, exist_ok=True)

    safe_name = city_name.lower().replace(" ", "_")
    file_name = f"{safe_name}_{execution_date}.json"
    file_path = os.path.join(archive_dir, file_name)

    if os.path.exists(file_path):
        log.info(
            "Archive déjà présente pour '%s' (%s) – archivage ignoré (idempotence). "
            "Chemin : %s",
            city_name,
            execution_date,
            file_path,
        )
        return file_path

    with open(file_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)

    log.info("Données archivées pour '%s' → %s", city_name, file_path)
    return file_path


# ──────────────────────────────────────────────────────────────────────────────
# Callable principal (PythonOperator)
# ──────────────────────────────────────────────────────────────────────────────
def extract_and_archive(**context: Any) -> None:
    """
    Tâche Airflow : extraction et archivage des données météo brutes.

    Étapes :
      1. Lecture des villes depuis la variable Airflow (ou valeurs par défaut)
      2. Appel API Open-Meteo pour chaque ville
      3. Archivage JSON avec garantie d'idempotence
      4. Push XCom :
         - ``archive_paths``   : dict {city_name: file_path}
         - ``cities_processed``: liste des villes traitées avec succès
         - ``execution_date``  : date d'exécution (ds)

    Lève RuntimeError si aucune ville n'a pu être extraite.
    """
    ti = context["ti"]
    execution_date: str = context["ds"]  # Format YYYY-MM-DD

    cities = _get_cities()
    archive_base_path = _get_archive_base_path()
    daily_archive_dir = os.path.join(archive_base_path, execution_date)

    log.info(
        "=== [extract_and_archive] Début | date=%s | villes=%d | archive=%s ===",
        execution_date,
        len(cities),
        daily_archive_dir,
    )

    archive_paths: Dict[str, str] = {}
    failed_cities: List[str] = []

    for city in cities:
        city_name: str = city["name"]
        try:
            raw_data = _fetch_weather(city)
            file_path = _archive_data(
                raw_data, daily_archive_dir, city_name, execution_date
            )
            archive_paths[city_name] = file_path

        except requests.exceptions.Timeout:
            log.error("Timeout API pour '%s' (> %ds).", city_name, REQUEST_TIMEOUT_SECONDS)
            failed_cities.append(city_name)
        except requests.exceptions.HTTPError as exc:
            log.error("Erreur HTTP pour '%s': %s", city_name, exc)
            failed_cities.append(city_name)
        except requests.exceptions.ConnectionError as exc:
            log.error("Erreur de connexion pour '%s': %s", city_name, exc)
            failed_cities.append(city_name)
        except OSError as exc:
            log.error("Erreur d'écriture archive pour '%s': %s", city_name, exc)
            failed_cities.append(city_name)
        except Exception as exc:  # noqa: BLE001
            log.error("Erreur inattendue pour '%s': %s", city_name, exc)
            failed_cities.append(city_name)

    if not archive_paths:
        raise RuntimeError(
            f"Extraction complètement échouée. Toutes les villes en erreur : {failed_cities}"
        )

    if failed_cities:
        log.warning(
            "Extraction partielle : %d/%d villes OK. Échecs : %s",
            len(archive_paths),
            len(cities),
            failed_cities,
        )

    log.info(
        "=== [extract_and_archive] Terminé : %d/%d villes extraites ===",
        len(archive_paths),
        len(cities),
    )

    ti.xcom_push(key="archive_paths", value=archive_paths)
    ti.xcom_push(key="execution_date", value=execution_date)
    ti.xcom_push(key="cities_processed", value=list(archive_paths.keys()))
