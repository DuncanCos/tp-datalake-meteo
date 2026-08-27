"""Producteur Kafka : poll Open-Meteo (meteo courante, 10 villes) -> topic weather.current.

Un message JSON par ville et par poll, clef = nom de ville.
Variables d'env : KAFKA_BOOTSTRAP, KAFKA_TOPIC, POLL_INTERVAL_SECONDS.
"""
import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
TOPIC = os.environ.get("KAFKA_TOPIC", "weather.current")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))

# lat/lon en chaines pour construire l'URL sans dependre de la locale
CITIES = [
    {"name": "Paris",      "dept": "75", "lat": "48.8566", "lon": "2.3522"},
    {"name": "Lyon",       "dept": "69", "lat": "45.7640", "lon": "4.8357"},
    {"name": "Marseille",  "dept": "13", "lat": "43.2965", "lon": "5.3698"},
    {"name": "Toulouse",   "dept": "31", "lat": "43.6047", "lon": "1.4442"},
    {"name": "Nice",       "dept": "06", "lat": "43.7102", "lon": "7.2620"},
    {"name": "Nantes",     "dept": "44", "lat": "47.2184", "lon": "-1.5536"},
    {"name": "Strasbourg", "dept": "67", "lat": "48.5734", "lon": "7.7521"},
    {"name": "Bordeaux",   "dept": "33", "lat": "44.8378", "lon": "-0.5792"},
    {"name": "Lille",      "dept": "59", "lat": "50.6292", "lon": "3.0573"},
    {"name": "Rennes",     "dept": "35", "lat": "48.1173", "lon": "-1.6778"},
]

CURRENT_VARS = ",".join([
    "temperature_2m", "relative_humidity_2m", "apparent_temperature", "is_day",
    "precipitation", "rain", "showers", "snowfall", "weather_code", "cloud_cover",
    "pressure_msl", "surface_pressure",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
])

URL = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={','.join(c['lat'] for c in CITIES)}"
    f"&longitude={','.join(c['lon'] for c in CITIES)}"
    f"&current={CURRENT_VARS}&timezone=UTC"
)


def make_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                linger_ms=100,
            )
        except NoBrokersAvailable:
            print("Kafka indisponible, nouvel essai dans 5 s...", flush=True)
            time.sleep(5)


def poll_once(producer: KafkaProducer) -> None:
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    resp = requests.get(URL, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    for city, block in zip(CITIES, payload):
        record = {
            "ingested_at": ingested_at,
            "city": city["name"],
            "dept": city["dept"],
            "latitude": block.get("latitude"),
            "longitude": block.get("longitude"),
            "elevation": block.get("elevation"),
            **block.get("current", {}),
        }
        producer.send(TOPIC, key=city["name"], value=record)
    producer.flush()
    print(f"{ingested_at} : {len(CITIES)} messages -> {TOPIC}", flush=True)


def main() -> None:
    producer = make_producer()
    print(f"Producteur demarre : {URL}", flush=True)
    while True:
        try:
            poll_once(producer)
        except Exception as exc:  # erreurs API transitoires : on garde le rythme
            print(f"poll en echec : {exc}", flush=True)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
