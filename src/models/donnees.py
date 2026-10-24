"""
Chargement du Gold et constantes communes à la partie modèle.
"""
from pathlib import Path

import pandas as pd

RACINE_PROJET = Path(__file__).resolve().parents[2]
CHEMIN_GOLD = RACINE_PROJET / "data" / "gold" / "jeu_apprentissage"

HORIZONS = [1, 3, 7]
SPLITS = ["train", "validation", "test", "test_2026"]


def charger_gold(chemin: Path = CHEMIN_GOLD) -> pd.DataFrame:
    """Lit le dossier Parquet du Gold, trié par date."""
    df = pd.read_parquet(chemin)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def extraire_bloc(df: pd.DataFrame, nom_split: str) -> pd.DataFrame:
    return df[df["split"] == nom_split]

FEATURES = (
    ["tm", "tn", "tx", "rr"]
    + [f"tm_j_moins_{k}" for k in (1, 2, 3, 7)]
    + ["tm_moy_3j", "tm_moy_7j", "jour_sin", "jour_cos"]
)


def ajouter_variables(bloc: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Variables dérivées des colonnes du Gold (aucune information du futur : J et le passé + normale)."""
    X = bloc[FEATURES].copy()
    X["normale_cible"] = bloc[f"ref_normale_j{horizon}"]
    X["ecart_normale"] = bloc["tm"] - bloc[f"ref_normale_j{horizon}"]
    X["variation_1j"] = bloc["tm"] - bloc["tm_j_moins_1"]
    X["amplitude"] = bloc["tx"] - bloc["tn"]
    return X


def separer_xy(
    bloc: pd.DataFrame, horizon: int, enrichi: bool = False, residuel: bool = False
) -> tuple[pd.DataFrame, pd.Series]:
    """X, y d'un bloc. residuel=True : y devient l'écart à la normale de J+h (on la rajoute après prédiction)."""
    X = ajouter_variables(bloc, horizon) if enrichi else bloc[FEATURES]
    y = bloc[f"cible_tm_j{horizon}"]
    if residuel:
        y = y - bloc[f"ref_normale_j{horizon}"]
    return X, y


def base_residuel(bloc: pd.DataFrame, horizon: int, residuel: bool):
    """Valeur à rajouter à la prédiction du modèle (la normale si residuel, sinon 0)."""
    return bloc[f"ref_normale_j{horizon}"].to_numpy() if residuel else 0.0
