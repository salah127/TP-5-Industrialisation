import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "dags"))

print("\n=== TEST EXTRACTOR ===")
from modules.extractor import _fetch_weather, _archive_data
city = {"name": "Paris", "latitude": 48.8566, "longitude": 2.3522}
data = _fetch_weather(city)
print("OK  temp=" + str(data["current_weather"]["temperature"]) + "C")
archive_dir = os.path.join(os.getcwd(), "data", "test")
archive_path = _archive_data(data, archive_dir, "Paris", "2024-01-15")
print("OK  archive=" + archive_path)

print("\n=== TEST TRANSFORMER ===")
from modules.transformer import _parse_city_record
record = _parse_city_record(data, "2024-01-15", inject_anomaly=False)
print("OK  " + record["city_name"] + " temp=" + str(record["temperature_2m"]))
record_bad = _parse_city_record(data, "2024-01-15", inject_anomaly=True)
print("OK  anomalie injectee temp=" + str(record_bad["temperature_2m"]) + " (attendu 999.0)")

print("\n=== TEST QUALITE (normal) ===")
from modules.quality import _check_record
is_valid, anomalies = _check_record(record)
print("OK  valid=" + str(is_valid))
assert is_valid, "ECHEC: record valide doit passer le QC"

print("\n=== TEST QUALITE (anomalie) ===")
is_valid, anomalies = _check_record(record_bad)
print("OK  anomalie detectee valid=" + str(is_valid))
print("    " + anomalies[0])
assert not is_valid, "ECHEC: 999C doit echouer le QC"

print("\n=== TOUS LES TESTS PASSENT ===")