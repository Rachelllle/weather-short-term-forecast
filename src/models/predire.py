"""
Prédit la température moyenne à J+1, J+3 et J+7 à partir de valeurs saisies.

Usage :
    python src/models/predire.py --date 2026-10-24 --tm 12.0 13.5 14.0 12.5 11.0 13.0 14.5 15.0 --tn 8.0 --tx 16.0 --rr 0.0

--tm : températures moyennes des 8 derniers jours, aujourd'hui en premier (J, J-1, ..., J-7)
"""
import argparse
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from donnees import FEATURES, HORIZONS, RACINE_PROJET  # noqa: E402


def construire_ligne(date: pd.Timestamp, tm: list[float], tn: float, tx: float, rr: float) -> pd.DataFrame:
    """Mêmes variables que le Gold (moyennes glissantes incluant le jour J)."""
    angle = 2 * np.pi * date.dayofyear / 365.25
    ligne = {
        "tm": tm[0], "tn": tn, "tx": tx, "rr": rr,
        "tm_j_moins_1": tm[1], "tm_j_moins_2": tm[2], "tm_j_moins_3": tm[3], "tm_j_moins_7": tm[7],
        "tm_moy_3j": np.mean(tm[:3]), "tm_moy_7j": np.mean(tm[:7]),
        "jour_sin": np.sin(angle), "jour_cos": np.cos(angle),
    }
    return pd.DataFrame([ligne])[FEATURES]


def main() -> None:
    parser = argparse.ArgumentParser(description="Prédiction de tm à J+1, J+3 et J+7")
    parser.add_argument("--date", required=True, help="Jour J, au format AAAA-MM-JJ")
    parser.add_argument("--tm", type=float, nargs=8, required=True, help="tm de J, J-1, ..., J-7")
    parser.add_argument("--tn", type=float, required=True, help="Température minimale du jour J")
    parser.add_argument("--tx", type=float, required=True, help="Température maximale du jour J")
    parser.add_argument("--rr", type=float, default=0.0, help="Pluie du jour J en mm (défaut : 0)")
    args = parser.parse_args()

    date = pd.Timestamp(args.date)
    X = construire_ligne(date, args.tm, args.tn, args.tx, args.rr)
    for h in HORIZONS:
        modele = joblib.load(RACINE_PROJET / "models" / f"xgboost_h{h}.joblib")
        jour_cible = (date + pd.Timedelta(days=h)).date()
        print(f"J+{h} ({jour_cible}) : {modele.predict(X)[0]:.1f} °C")


if __name__ == "__main__":
    main()
