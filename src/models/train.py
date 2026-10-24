"""
Entraînement : un modèle par horizon (stratégie directe).

  1. entraînement sur train, scoring sur validation (recherche d'hyperparamètres sur validation uniquement)
  2. refit du meilleur modèle de chaque horizon sur train + validation
  3. évaluation UNE SEULE FOIS sur test et test_2026, contre les deux baselines

Sorties : models/{modele}_h{h}.joblib et results/metrics/scores.csv

Usage :
    python src/models/train.py
"""
import sys
from itertools import product
from pathlib import Path

import joblib
import pandas as pd
from sklearn.base import clone
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

sys.path.insert(0, str(Path(__file__).resolve().parent))
from baselines import TYPES, predire_baseline  # noqa: E402
from donnees import HORIZONS, RACINE_PROJET, charger_gold, extraire_bloc, separer_xy  # noqa: E402
from metrics import mae, rmse, skill_score  # noqa: E402

SEED = 42
DOSSIER_MODELES = RACINE_PROJET / "models"
CHEMIN_SCORES = RACINE_PROJET / "results" / "metrics" / "scores.csv"
BLOCS_TEST = ["test", "test_2026"]

GRILLES = {
    "ridge": {"alpha": [0.01, 0.1, 1, 10, 100]},
    "xgboost": {"max_depth": [2, 3, 5], "n_estimators": [100, 300], "learning_rate": [0.03, 0.1]},
    "random_forest": {"max_depth": [5, 10, None]},
}


def creer_modele(nom: str, params: dict):
    if nom == "ridge":
        return Pipeline([("scaler", StandardScaler()), ("ridge", Ridge(random_state=SEED, **params))])
    if nom == "xgboost":
        return XGBRegressor(random_state=SEED, n_jobs=1, **params)
    if nom == "random_forest":
        return RandomForestRegressor(n_estimators=300, random_state=SEED, n_jobs=1, **params)
    raise ValueError(f"Modèle inconnu : {nom}")


def combinaisons(grille: dict) -> list[dict]:
    return [dict(zip(grille, valeurs)) for valeurs in product(*grille.values())]


def ligne_scores(horizon: int, modele: str, bloc: str, y, y_pred, refs: dict) -> dict:
    """Une ligne de scores.csv : erreurs + skill contre les deux baselines."""
    r = rmse(y, y_pred)
    return {
        "horizon": horizon,
        "modele": modele,
        "bloc": bloc,
        "mae": mae(y, y_pred),
        "rmse": r,
        "skill_vs_persistance": skill_score(r, refs["persistance"]),
        "skill_vs_normale": skill_score(r, refs["saisonniere"]),
    }


def rmse_baselines(bloc: pd.DataFrame, horizon: int) -> dict:
    y = bloc[f"cible_tm_j{horizon}"]
    return {t: rmse(y, predire_baseline(bloc, horizon, t)) for t in TYPES}


def main() -> None:
    df = charger_gold()
    train, val = extraire_bloc(df, "train"), extraire_bloc(df, "validation")
    train_val = pd.concat([train, val])
    lignes = []

    for h in HORIZONS:
        X_train, y_train = separer_xy(train, h)
        X_val, y_val = separer_xy(val, h)
        refs_val = rmse_baselines(val, h)

        meilleurs = {}
        for nom, grille in GRILLES.items():
            for params in combinaisons(grille):
                modele = creer_modele(nom, params).fit(X_train, y_train)
                r = rmse(y_val, modele.predict(X_val))
                if nom not in meilleurs or r < meilleurs[nom][0]:
                    meilleurs[nom] = (r, params)
        for nom, (_, params) in meilleurs.items():
            modele = creer_modele(nom, params).fit(X_train, y_train)
            lignes.append(ligne_scores(h, nom, "validation", y_val, modele.predict(X_val), refs_val))
        for t in TYPES:
            lignes.append(ligne_scores(h, t, "validation", y_val, predire_baseline(val, h, t), refs_val))

        gagnant = min(meilleurs, key=lambda n: meilleurs[n][0])
        params = meilleurs[gagnant][1]
        print(f"h{h} : meilleur modèle = {gagnant} {params} (RMSE validation = {meilleurs[gagnant][0]:.3f})")

        X_tv, y_tv = separer_xy(train_val, h)
        final = creer_modele(gagnant, params).fit(X_tv, y_tv)
        DOSSIER_MODELES.mkdir(exist_ok=True)
        joblib.dump(final, DOSSIER_MODELES / f"{gagnant}_h{h}.joblib")

        for nom_bloc in BLOCS_TEST:
            bloc = extraire_bloc(df, nom_bloc)
            X, y = separer_xy(bloc, h)
            refs = rmse_baselines(bloc, h)
            lignes.append(ligne_scores(h, gagnant, nom_bloc, y, final.predict(X), refs))
            for t in TYPES:
                lignes.append(ligne_scores(h, t, nom_bloc, y, predire_baseline(bloc, h, t), refs))

    scores = pd.DataFrame(lignes)
    CHEMIN_SCORES.parent.mkdir(parents=True, exist_ok=True)
    scores.to_csv(CHEMIN_SCORES, index=False)
    print(f"\n{CHEMIN_SCORES.relative_to(RACINE_PROJET)}")
    print(scores[scores["bloc"].isin(BLOCS_TEST)].round(3).to_string(index=False))


if __name__ == "__main__":
    main()
