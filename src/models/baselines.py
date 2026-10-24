"""
Baselines de référence, lues directement dans le Gold :
- persistance : la prédiction à J+h est tm du jour J (colonne ref_persistance)
- saisonnière : la prédiction à J+h est la normale de la date J+h (colonne ref_normale_j{h})

Usage :
    python src/models/baselines.py
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from donnees import HORIZONS, SPLITS, charger_gold, extraire_bloc  # noqa: E402

TYPES = ["persistance", "saisonniere"]


def predire_baseline(bloc: pd.DataFrame, horizon: int, type: str) -> np.ndarray:
    if type == "persistance":
        return bloc["ref_persistance"].to_numpy()
    if type == "saisonniere":
        return bloc[f"ref_normale_j{horizon}"].to_numpy()
    raise ValueError(f"Type de baseline inconnu : {type} (attendu : {TYPES})")


def main() -> None:
    df = charger_gold()
    for nom in SPLITS:
        bloc = extraire_bloc(df, nom)
        for h in HORIZONS:
            y = bloc[f"cible_tm_j{h}"].to_numpy()
            for type in TYPES:
                erreur = predire_baseline(bloc, h, type) - y
                mae = np.mean(np.abs(erreur))
                rmse = np.sqrt(np.mean(erreur**2))
                print(f"{nom:<10} h{h}  {type:<12} MAE={mae:.3f}  RMSE={rmse:.3f}")


if __name__ == "__main__":
    main()
