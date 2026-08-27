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


def read_gold(table: str, subdir: str = "", ttl: int = CACHE_TTL) -> pd.DataFrame:
    """Lit une table Gold (ou une de ses partitions) avec cache TTL."""
    key = f"{table}/{subdir}"
    now = time.time()
    if key in _cache and now - _cache[key][0] < ttl:
        return _cache[key][1]
    path = f"{GOLD}/{table}/{subdir}".rstrip("/")
    frames = []
    try:
        for root, _dirs, files in client.walk(path):
            for f in files:
                if f.endswith(".parquet"):
                    with client.read(f"{root}/{f}") as reader:
                        frames.append(pd.read_parquet(io.BytesIO(reader.read())))
    except Exception:
        # table pas encore construite par les jobs Gold -> DataFrame vide,
        # les endpoints repondent [] au lieu d'une 500
        pass
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    _cache[key] = (now, df)
    return df


def daily_year_dirs() -> list[str]:
    """Partitions year=YYYY de grisaille_daily, triees."""
    try:
        return sorted(d for d in client.list(f"{GOLD}/grisaille_daily")
                      if d.startswith("year="))
    except Exception:
        return []


def records(df: pd.DataFrame) -> list[dict]:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].astype(str)
        elif out[col].dtype == object:
            out[col] = out[col].astype(str)
    # passage en object AVANT le where : sur des colonnes float, None serait
    # re-coerce en NaN et ferait planter la serialisation JSON
    out = out.astype(object).where(pd.notna(out), None)
    return out.to_dict(orient="records")


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
    # la fenetre du graphe tient dans les 2 dernieres annees : on ne lit
    # que ces partitions, pas les ~130 annees d'historique
    years = daily_year_dirs()[-2:]
    frames = [read_gold("grisaille_daily", y, ttl=600) for y in years]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return []
    df = pd.concat(frames, ignore_index=True)
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    cutoff = df["obs_date"].max() - pd.Timedelta(days=days)
    sel = df[df["obs_date"] >= cutoff][
        ["obs_date", "city", "dept", "grisaille", "temp_avg", "temp_min",
         "temp_max", "precip_mm", "sunshine_min", "wind_ms"]
    ].sort_values(["city", "obs_date"])
    return records(sel)


@app.get("/api/podiums")
def podiums():
    """Classements par periode : mois, annee, 5 ans, 10 ans."""
    df = read_gold("grisaille_podiums")
    if df.empty:
        return {}
    out = {}
    for ptype, sub in df.groupby("period_type"):
        out[ptype] = {
            "label": sub["period_label"].iloc[0],
            "villes": records(sub.sort_values("rang_misere")),
        }
    return out


@app.get("/api/dates")
def dates():
    """Liste des journees disponibles (cache long).

    Lit la petite table dediee grisaille_dates ecrite par le job Gold ;
    repli sur un scan complet de grisaille_daily si elle n'existe pas encore.
    """
    df = read_gold("grisaille_dates", ttl=1800)
    if df.empty:
        df = read_gold("grisaille_daily", ttl=1800)
    if df.empty:
        return []
    return sorted(pd.to_datetime(df["obs_date"]).dt.strftime("%Y-%m-%d").unique().tolist())


@app.get("/api/day")
def day(date: str):
    """Les 10 villes pour une journee donnee (YYYY-MM-DD) — lit 1 partition."""
    df = read_gold("grisaille_daily", f"year={date[:4]}", ttl=600)
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
    # version enrichie du croisement vigilance si le job confrontation a
    # deja tourne, sinon repli sur la table episodes de base
    df = read_gold("episodes_vigilance")
    if df.empty:
        df = read_gold("episodes")
    if df.empty:
        return []
    df = df.sort_values("date_fin", ascending=False).head(limit)
    return records(df)


# ── grille SIM2 (heatmap) ──────────────────────────────────────────────
# cellules compactes : [lat, lon, temp, precip_mm, wind_ms, humidity_pct,
#                       ssi, grisaille]

GRID_DAILY_COLS = ["lat", "lon", "temp", "precip_mm", "wind_ms",
                   "humidity_pct", "ssi", "grisaille"]


def cell_rows(df: pd.DataFrame, cols: list[str]) -> list[list]:
    out = df[cols].astype(object).where(pd.notna(df[cols]), None)
    return out.values.tolist()


@app.get("/api/grid")
def grid(date: str):
    """Cellules 16 km de la grille SIM2 quotidienne pour une journee."""
    subdir = f"year={int(date[:4])}/month={int(date[5:7])}"
    df = read_gold("grisaille_grid_daily", subdir, ttl=600)
    if df.empty:
        return {"date": date, "resolution_km": 16, "cells": []}
    df = df.copy()
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.strftime("%Y-%m-%d")
    sel = df[df["obs_date"] == date]
    return {"date": date, "resolution_km": 16,
            "cells": cell_rows(sel, GRID_DAILY_COLS)}


@app.get("/api/grid_month")
def grid_month(year: int, month: int):
    """Cellules 16 km de la grille SIM2 mensuelle (indice partiel)."""
    df = read_gold("grisaille_grid_monthly", f"year={year}", ttl=600)
    if df.empty:
        return {"year": year, "month": month, "resolution_km": 16, "cells": []}
    sel = df[df["month"] == month]
    # meme gabarit que la grille quotidienne : vent/humidite/ssi inconnus
    rows = cell_rows(sel, ["lat", "lon", "temp", "precip_mm", "grisaille_partielle"])
    cells = [[r[0], r[1], r[2], r[3], None, None, None, r[4]] for r in rows]
    return {"year": year, "month": month, "resolution_km": 16, "cells": cells}


@app.get("/api/grid_dates")
def grid_dates():
    """Fenetre quotidienne disponible + annees de la grille mensuelle."""
    daily = read_gold("grisaille_grid_daily", ttl=1800)
    days = (sorted(pd.to_datetime(daily["obs_date"]).dt.strftime("%Y-%m-%d")
                   .unique().tolist()) if not daily.empty else [])
    try:
        years = sorted(int(d.split("=")[1])
                       for d in client.list(f"{GOLD}/grisaille_grid_monthly")
                       if d.startswith("year="))
    except Exception:
        years = []
    return {"days": days, "monthly_years": years}


# ── confrontation live vs officiel ─────────────────────────────────────

@app.get("/api/live_vs_official")
def live_vs_official(days: int = 14):
    """Ecarts quotidiens Open-Meteo vs officiel Meteo-France, par ville."""
    df = read_gold("live_vs_official", ttl=300)
    if df.empty:
        return []
    df = df.copy()
    df["obs_day"] = pd.to_datetime(df["obs_day"])
    cutoff = df["obs_day"].max() - pd.Timedelta(days=days)
    sel = df[df["obs_day"] >= cutoff].sort_values(["city", "obs_day"]).copy()
    sel["obs_day"] = sel["obs_day"].dt.strftime("%Y-%m-%d")
    return records(sel)


@app.get("/api/reliability")
def reliability():
    """Synthese fiabilite du live par ville (MAE / biais)."""
    df = read_gold("live_reliability", ttl=300)
    if df.empty:
        return []
    return records(df.sort_values("mae_temp"))


app.mount("/", StaticFiles(directory="static", html=True), name="static")
