"""
Métriques d'évaluation : MAE, RMSE et skill score.

Usage :
    python src/models/metrics.py
"""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from baselines import TYPES, predire_baseline  # noqa: E402
from donnees import HORIZONS, charger_gold, extraire_bloc  # noqa: E402


def mae(y_vrai, y_pred) -> float:
    return float(np.mean(np.abs(np.asarray(y_pred) - np.asarray(y_vrai))))


def rmse(y_vrai, y_pred) -> float:
    return float(np.sqrt(np.mean((np.asarray(y_pred) - np.asarray(y_vrai)) ** 2)))


def skill_score(rmse_modele: float, rmse_ref: float) -> float:
    """1 - rmse_modele / rmse_ref : > 0 si le modèle bat la référence, 1 = parfait, 0 = égal."""
    return 1 - rmse_modele / rmse_ref


def main() -> None:
    test = extraire_bloc(charger_gold(), "test")
    for h in HORIZONS:
        y = test[f"cible_tm_j{h}"]
        rmses = {t: rmse(y, predire_baseline(test, h, t)) for t in TYPES}
        skill = skill_score(rmses["saisonniere"], rmses["persistance"])
        print(f"test h{h}  RMSE persistance={rmses['persistance']:.3f}  "
              f"saisonniere={rmses['saisonniere']:.3f}  skill saisonniere vs persistance={skill:+.3f}")


if __name__ == "__main__":
    main()
