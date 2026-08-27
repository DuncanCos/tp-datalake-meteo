# Analyse exploratoire des fichiers Meteo-France (janvier 2026)
import glob
import os

import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data_sample", "meteofrance")
files = sorted(glob.glob(os.path.join(DATA_DIR, "Q_*_latest-2025-2026_RR-T-Vent.csv.gz")))

frames = []
for f in files:
    dept = os.path.basename(f).split("_")[1]
    df = pd.read_csv(f, sep=";", compression="gzip", dtype={"NUM_POSTE": str})
    df["DEPT"] = dept
    frames.append(df)

full = pd.concat(frames, ignore_index=True)
print("=== Fichiers RR-T-Vent 2025-2026, 10 departements ===")
print(f"Lignes totales : {len(full):,}  |  Colonnes : {len(full.columns)}")
print(f"Periode couverte : {full['AAAAMMJJ'].min()} -> {full['AAAAMMJJ'].max()}")
print(f"Stations distinctes : {full['NUM_POSTE'].nunique()}")

print("\nColonnes :", list(full.columns))

jan = full[(full["AAAAMMJJ"] >= 20260101) & (full["AAAAMMJJ"] <= 20260131)].copy()
print("\n=== Focus janvier 2026 ===")
print(f"Lignes : {len(jan):,}  |  Stations : {jan['NUM_POSTE'].nunique()}")

print("\nStations par departement (janvier 2026) :")
print(jan.groupby("DEPT")["NUM_POSTE"].nunique().to_string())

core = ["RR", "TN", "TX", "TM", "FFM", "FXY", "FXI", "DG"]
print("\nTaux de remplissage janvier 2026 (colonnes cles, %) :")
print((jan[core].notna().mean() * 100).round(1).to_string())

print("\nStats janvier 2026 (unites : RR en mm, T en degC, vent en m/s) :")
print(jan[core].describe().round(2).to_string())

print("\nRepartition des codes qualite (QRR, QTN, QTX) :")
for q in ["QRR", "QTN", "QTX"]:
    vc = jan[q].value_counts(dropna=False).to_dict()
    print(f"  {q}: {vc}")

print("\nTemperature moyenne TM par departement en janvier 2026 :")
agg = jan.groupby("DEPT").agg(
    tm_moy=("TM", "mean"), tn_min=("TN", "min"), tx_max=("TX", "max"),
    rr_cumul_moy_station=("RR", lambda s: s.sum() / jan[jan["RR"].notna()].groupby("DEPT")["NUM_POSTE"].nunique().get(s.name, 1) if False else s.mean() * 31),
    jours_gel=("DG", lambda s: (s.fillna(0) > 0).sum()),
).round(1)
print(agg.to_string())

print("\nExemple de lignes (station de Paris-Montsouris si presente) :")
mont = jan[jan["NOM_USUEL"].str.contains("MONTSOURIS", case=False, na=False)]
cols_show = ["NUM_POSTE", "NOM_USUEL", "AAAAMMJJ", "RR", "QRR", "TN", "TX", "TM", "FFM", "FXY"]
print((mont if len(mont) else jan.head(5))[cols_show].head(8).to_string(index=False))

# fichier autres-parametres (dept 75) pour voir la richesse
f75 = os.path.join(DATA_DIR, "Q_75_latest-2025-2026_autres-parametres.csv.gz")
ap = pd.read_csv(f75, sep=";", compression="gzip", dtype={"NUM_POSTE": str})
ap_jan = ap[(ap["AAAAMMJJ"] >= 20260101) & (ap["AAAAMMJJ"] <= 20260131)]
interesting = ["INST", "GLOT", "UM", "UN", "UX", "PMERM", "NEIG", "BROU", "ORAG", "GELEE"]
avail = [c for c in interesting if c in ap_jan.columns]
print("\n=== Autres parametres (dept 75, janvier 2026) ===")
print(f"Lignes : {len(ap_jan)}  |  Stations : {ap_jan['NUM_POSTE'].nunique()}")
print("Taux de remplissage (%) :")
print((ap_jan[avail].notna().mean() * 100).round(1).to_string())
