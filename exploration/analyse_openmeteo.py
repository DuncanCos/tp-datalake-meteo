# Analyse exploratoire de la capture temps reel Open-Meteo (JSONL)
import json
import os

import pandas as pd

path = os.path.join(os.path.dirname(__file__), "..", "data_sample", "openmeteo", "current_capture.jsonl")

records = []
with open(path, encoding="utf-8-sig") as fh:
    for line in fh:
        line = line.strip().lstrip("﻿")
        if not line:
            continue
        r = json.loads(line)
        flat = {k: v for k, v in r.items() if k != "current"}
        flat.update(r["current"])
        records.append(flat)

df = pd.DataFrame(records)
print("=== Capture Open-Meteo ===")
print(f"Lignes : {len(df)}  |  Polls : {df['poll'].nunique()}  |  Villes : {df['city'].nunique()}")
print(f"Fenetre : {df['ingested_at'].min()} -> {df['ingested_at'].max()}")
print("\nColonnes :", list(df.columns))

print("\nHorodatage API 'time' distincts par ville (fraicheur des donnees, pas de 15 min) :")
print(df.groupby("city")["time"].nunique().to_string())

meas = ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation",
        "rain", "snowfall", "weather_code", "cloud_cover", "pressure_msl",
        "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"]
print("\nTaux de remplissage (%) :")
print((df[meas].notna().mean() * 100).round(1).to_string())

print("\nDernier poll — apercu par ville :")
last = df[df["poll"] == df["poll"].max()]
cols = ["city", "time", "temperature_2m", "relative_humidity_2m", "precipitation",
        "weather_code", "cloud_cover", "wind_speed_10m", "wind_gusts_10m"]
print(last[cols].to_string(index=False))

print("\nVariation de temperature sur la fenetre par ville (min/max) :")
print(df.groupby("city")["temperature_2m"].agg(["min", "max", "nunique"]).to_string())
