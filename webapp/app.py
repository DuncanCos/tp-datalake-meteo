"""API du site Meteo Grisaille : lit les tables Gold (Parquet) via WebHDFS.

Endpoints JSON + frontend statique. Cache memoire 60 s par table pour ne pas
marteler HDFS a chaque visiteur.
"""
import io
import os
import time

import pandas as pd
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from hdfs import InsecureClient

WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "http://namenode:9870")
GOLD = "/datalake/gold"
CACHE_TTL = 60  # secondes

app = FastAPI(title="Meteo Grisaille API")
client = InsecureClient(WEBHDFS_URL, user="root")
_cache: dict[str, tuple[float, pd.DataFrame]] = {}


def read_gold(table: str) -> pd.DataFrame:
    now = time.time()
    if table in _cache and now - _cache[table][0] < CACHE_TTL:
        return _cache[table][1]
    frames = []
    for root, _dirs, files in client.walk(f"{GOLD}/{table}"):
        for f in files:
            if f.endswith(".parquet"):
                with client.read(f"{root}/{f}") as reader:
                    frames.append(pd.read_parquet(io.BytesIO(reader.read())))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    _cache[table] = (now, df)
    return df


def records(df: pd.DataFrame) -> list[dict]:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].astype(str)
        elif out[col].dtype == object:
            out[col] = out[col].astype(str)
    return out.where(pd.notna(out), None).to_dict(orient="records")


@app.get("/api/live")
def live():
    df = read_gold("live_status").sort_values("rang_misere_live")
    # date de derniere ecriture de la table par le job Gold (fraicheur pipeline)
    mtime = 0
    for root, _dirs, files in client.walk(f"{GOLD}/live_status"):
        for f in files:
            st = client.status(f"{root}/{f}", strict=False)
            if st:
                mtime = max(mtime, st["modificationTime"])
    return {"updated_at": mtime, "villes": records(df)}


@app.get("/api/ranking")
def ranking(year: int | None = None, month: int | None = None):
    df = read_gold("grisaille_ranking")
    if df.empty:
        return []
    if year is None or month is None:
        last = df.sort_values(["year", "month"]).iloc[-1]
        year, month = int(last["year"]), int(last["month"])
    sel = df[(df["year"] == year) & (df["month"] == month)]
    return {"year": year, "month": month,
            "villes": records(sel.sort_values("rang_misere"))}


@app.get("/api/daily")
def daily(days: int = 30):
    df = read_gold("grisaille_daily")
    if df.empty:
        return []
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    cutoff = df["obs_date"].max() - pd.Timedelta(days=days)
    sel = df[df["obs_date"] >= cutoff][
        ["obs_date", "city", "dept", "grisaille", "temp_avg", "temp_min",
         "temp_max", "precip_mm", "sunshine_min", "wind_ms"]
    ].sort_values(["city", "obs_date"])
    return records(sel)


@app.get("/api/dates")
def dates():
    """Liste des journees disponibles dans grisaille_daily."""
    df = read_gold("grisaille_daily")
    if df.empty:
        return []
    return sorted(pd.to_datetime(df["obs_date"]).dt.strftime("%Y-%m-%d").unique().tolist())


@app.get("/api/day")
def day(date: str):
    """Les 10 villes pour une journee donnee (YYYY-MM-DD)."""
    df = read_gold("grisaille_daily")
    if df.empty:
        return []
    df = df.copy()
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.strftime("%Y-%m-%d")
    cols = ["obs_date", "city", "dept", "grisaille", "temp_avg", "temp_min",
            "temp_max", "precip_mm", "wind_ms", "gust_ms", "sunshine_min",
            "humidity_pct", "fog", "n_stations"]
    sel = df[df["obs_date"] == date][[c for c in cols if c in df.columns]]
    return records(sel.sort_values("city"))


@app.get("/api/episodes")
def episodes(limit: int = 30):
    df = read_gold("episodes")
    if df.empty:
        return []
    df = df.sort_values("date_fin", ascending=False).head(limit)
    return records(df)


app.mount("/", StaticFiles(directory="static", html=True), name="static")
