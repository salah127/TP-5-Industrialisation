# Pipeline Airflow Open-Meteo - TP5

## Description du pipeline

Pipeline de donnees orchestre avec Apache Airflow qui :
1. **Extrait** des donnees meteo depuis l'API Open-Meteo pour plusieurs villes configurables
2. **Archive** les reponses JSON brutes (tracabilite des donnees sources)
3. **Transforme** les donnees brutes en enregistrements structures
4. **Controle la qualite** selon des regles metier (plages de valeurs, champs obligatoires)
5. **Branche** conditionnellement : charge dans PostgreSQL si qualite OK, sinon trace l'anomalie
6. **Charge** les donnees avec insertion idempotente (`INSERT ... ON CONFLICT DO UPDATE`)

---

## Schema du workflow

```
extract_and_archive
        |
   transform_data
        |
 check_data_quality
        |
  branch_on_quality
     /         \
[OK]            [ANOMALIE]
load_postgres   log_anomaly
     \         /
       end
```

---

## Structure du projet

```
TP 5/
├── dags/
│   ├── open_meteo_pipeline.py     <- DAG principal
│   └── modules/
│       ├── extractor.py           <- Extraction API + archivage
│       ├── transformer.py         <- Transformation des donnees
│       ├── quality.py             <- Controle qualite + branchement
│       ├── loader.py              <- Chargement PostgreSQL (idempotent)
│       └── anomaly.py             <- Journalisation des anomalies
├── sql/
│   ├── create_tables.sql          <- Creation des tables
│   └── upsert_weather.sql         <- INSERT ... ON CONFLICT DO UPDATE
├── dashboard.html                 <- [BONUS] Dashboard meteo en direct
├── test_pipeline.py               <- Test des modules sans Airflow
└── README.md
```

---

## Variables Airflow utilisees

| Variable | Description | Exemple |
|---|---|---|
| `open_meteo_cities` | Liste JSON des villes | `[{"name":"Paris","latitude":48.8566,"longitude":2.3522}]` |
| `open_meteo_archive_path` | Repertoire d'archivage | `/opt/airflow/data/archive` |
| `open_meteo_simulate_anomaly` | Active la simulation d'anomalie | `false` |

```bash
airflow variables set open_meteo_cities '[{"name":"Paris","latitude":48.8566,"longitude":2.3522},{"name":"Lyon","latitude":45.764,"longitude":4.8357},{"name":"Marseille","latitude":43.2965,"longitude":5.3698}]'
airflow variables set open_meteo_archive_path /opt/airflow/data/archive
airflow variables set open_meteo_simulate_anomaly false
```

---

## Connexions Airflow utilisees

| Connexion | Type | Description |
|---|---|---|
| `postgres_weather` | Postgres | Base de donnees PostgreSQL cible |

```bash
airflow connections add postgres_weather \
  --conn-type postgres --conn-host localhost --conn-port 5432 \
  --conn-login weather_user --conn-password weather_pass --conn-schema weather_db
```

---

## Description des taches du DAG

| Tache | Type | Description |
|---|---|---|
| `extract_and_archive` | PythonOperator | Appelle l'API Open-Meteo, archive le JSON brut. Ignore si le fichier existe (idempotence). |
| `transform_data` | PythonOperator | Parse le JSON, extrait temperature/vent/direction/weathercode. |
| `check_data_quality` | PythonOperator | Applique les regles QC, produit `quality_passed` (XCom). |
| `branch_on_quality` | BranchPythonOperator | Lit `quality_passed` et route vers `load_postgres` ou `log_anomaly`. |
| `load_postgres` | PythonOperator | INSERT idempotent dans `weather_current` + ligne dans `ingestion_log`. |
| `log_anomaly` | PythonOperator | Log structure des anomalies + ligne dans `ingestion_log`. Ne charge pas les donnees. |
| `end` | EmptyOperator | Tache terminale (`NONE_FAILED_MIN_ONE_SUCCESS`). |

---

## Strategie de robustesse

| Mecanisme | Valeur |
|---|---|
| `retries` | 2 |
| `retry_delay` | 5 minutes |
| `execution_timeout` | 30 minutes |
| Timeout HTTP | 30 secondes |
| `max_active_runs` | 1 |
| Gestion d'erreurs | `try/except` par type d'exception dans chaque module |

---

## Strategie d'idempotence

- **Archivage** : si le fichier JSON existe deja pour la ville et la date, l'appel API est ignore.
- **Base de donnees** : contrainte `UNIQUE (city_name, execution_date)` + `ON CONFLICT DO UPDATE` -> une relance met a jour sans creer de doublon.

```sql
-- Verification absence de doublons
SELECT city_name, execution_date, COUNT(*) FROM weather_current
GROUP BY city_name, execution_date HAVING COUNT(*) > 1;
```

---

## Controles qualite mis en place

| Champ | Regle |
|---|---|
| `temperature_2m` | Entre -50 C et +60 C |
| `windspeed_10m` | Entre 0 et 300 km/h |
| `winddirection_10m` | Entre 0 et 360 degres |
| `city_name`, `execution_date`, `temperature_2m`, `windspeed_10m`, `observation_time` | Champs obligatoires non vides |

---

## Regle de branchement conditionnel

Le `BranchPythonOperator` lit `quality_passed` depuis XCom :
- `True`  -> execute `load_postgres`, met `log_anomaly` en SKIPPED
- `False` -> execute `log_anomaly`, met `load_postgres` en SKIPPED

---

## Description des logs produits

| Tache | Niveau | Message type |
|---|---|---|
| `extract_and_archive` | INFO | `Donnees archivees pour 'Paris' -> .../paris_2024-01-15.json` |
| `transform_data` | WARNING | `INJECTION ANOMALIE pour 'Paris': temperature forcee a 999.0 C` |
| `check_data_quality` | WARNING | `[Paris] temperature_2m=999.0 hors plage [-50.0, 60.0]` |
| `branch_on_quality` | INFO/WARNING | `Branchement -> 'load_postgres'` ou `Branchement -> 'log_anomaly'` |
| `load_postgres` | INFO | `Chargement OK pour 'Paris' - INSERT OR UPDATE applique.` |
| `log_anomaly` | WARNING | `ANOMALIE QUALITE DETECTEE` + detail + `chargement BLOQUE` |

---

## Description des tables PostgreSQL

### `weather_current`

| Colonne | Type | Description |
|---|---|---|
| `id` | SERIAL | Cle primaire |
| `city_name` | VARCHAR(100) | Nom de la ville |
| `execution_date` | DATE | Date d'execution du DAG |
| `temperature_2m` | NUMERIC(7,2) | Temperature en C |
| `windspeed_10m` | NUMERIC(7,2) | Vitesse du vent en km/h |
| `winddirection_10m` | NUMERIC(7,2) | Direction en degres |
| `weathercode` | INTEGER | Code WMO |
| `is_day` | BOOLEAN | Mesure en journee |
| `observation_time` | VARCHAR(30) | Horodatage de la mesure |
| `quality_status` | VARCHAR(20) | `valid` |
| `ingested_at` | TIMESTAMP | Premier chargement |
| `updated_at` | TIMESTAMP | Derniere mise a jour |

Contrainte : `UNIQUE (city_name, execution_date)`

### `ingestion_log`

| Colonne | Type | Description |
|---|---|---|
| `id` | SERIAL | Cle primaire |
| `dag_id` | VARCHAR(200) | ID du DAG |
| `run_id` | VARCHAR(500) | ID du run |
| `execution_date` | DATE | Date d'execution |
| `status` | VARCHAR(50) | `success` / `partial` / `quality_failure` |
| `cities_processed` | INTEGER | Nombre de villes traitees |
| `quality_passed` | BOOLEAN | Resultat QC |
| `anomaly_details` | TEXT | Detail des anomalies (NULL si succes) |
| `created_at` | TIMESTAMP | Horodatage |

---

## Preuves d'execution

### Cas nominal - execution reussie
> _[Capture d'ecran du Graph View : toutes les taches en vert, `log_anomaly` en SKIPPED]_

### Cas anomalie qualite
> _[Capture d'ecran du Graph View : `load_postgres` en SKIPPED, `log_anomaly` en SUCCESS]_

> _[Capture d'ecran des logs Airflow de la tache `log_anomaly`]_

**Simulation** : Admin -> Variables -> `open_meteo_simulate_anomaly` = `true`, puis declencher le DAG. La tache `transform_data` injecte 999 C -> regle QC [-50, +60] declenchee -> branchement vers `log_anomaly`.

### Cas relance sans doublon (idempotence)
> _[Capture d'ecran de la requete `HAVING COUNT(*) > 1` retournant 0 ligne]_

> _[Capture d'ecran montrant `ingested_at` != `updated_at` apres relance]_

### Contenu des tables PostgreSQL
> _[Capture d'ecran du contenu de `weather_current` et `ingestion_log`]_

---

## Dashboard meteo (bonus)

Le fichier `dashboard.html` est un bonus visuel independant du pipeline Airflow.
Il appelle directement l'API Open-Meteo et affiche :
- Temperature, ressenti, humidite, vent, UV, probabilite de precipitations
- Badge de controle qualite (memes regles que le pipeline)
- Previsions 7 jours pour Paris
- Rafraichissement automatique toutes les 30 secondes

**Ouverture :**
```powershell
Start-Process "dashboard.html"
```

> Note : ce dashboard n'est pas lie a PostgreSQL. Il ne lit pas les donnees chargees
> par le pipeline - c'est un outil de demonstration visuelle independant.

---

## Test des modules (sans Airflow)

```powershell
py test_pipeline.py
```

Teste directement : extraction API, archivage, transformation, controle qualite (cas normal + cas anomalie).
Ne necessite pas Airflow ni PostgreSQL.

---

## Limites eventuelles

1. **Granularite journaliere** : la cle `(city_name, execution_date)` ne conserve qu'une observation par ville par jour. Un pipeline horaire necessiterait d'inclure l'heure.
2. **Donnees horaires non exploitees** : l'API retourne aussi des series horaires non utilisees ici.
3. **Alertes email** desactivees (pas de config SMTP en developpement).
4. **Pas de tests unitaires formels** : les fonctions `_check_record` et `_parse_city_record` sont testables avec `pytest`.
5. **Dashboard non connecte a PostgreSQL** : le frontend appelle l'API directement, il ne reflete pas les donnees chargees par le pipeline.