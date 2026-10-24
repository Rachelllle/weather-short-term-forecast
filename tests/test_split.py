"""
Tests du découpage chronologique du Gold : pas de chevauchement, gap anti-fuite, pas d'information du futur.

Usage :
    pytest tests/test_split.py
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "models"))
from donnees import CHEMIN_GOLD, HORIZONS, SPLITS, charger_gold, extraire_bloc  # noqa: E402

GAP = max(HORIZONS)
RETARDS = {"tm_j_moins_1": 1, "tm_j_moins_2": 2, "tm_j_moins_3": 3, "tm_j_moins_7": 7}

pytestmark = pytest.mark.skipif(not CHEMIN_GOLD.exists(), reason="Gold absent : lance silver.py puis gold.py")


@pytest.fixture(scope="module")
def gold() -> pd.DataFrame:
    return charger_gold()


def test_aucune_date_commune(gold):
    dates = {nom: set(extraire_bloc(gold, nom)["date"]) for nom in SPLITS}
    for i, a in enumerate(SPLITS):
        for b in SPLITS[i + 1:]:
            assert not dates[a] & dates[b], f"Dates communes entre {a} et {b}"


def test_gap_entre_blocs(gold):
    for avant, apres in zip(SPLITS, SPLITS[1:]):
        fin = extraire_bloc(gold, avant)["date"].max()
        debut = extraire_bloc(gold, apres)["date"].min()
        assert fin + pd.Timedelta(days=GAP) < debut, f"Gap insuffisant entre {avant} et {apres}"


@pytest.mark.parametrize("colonne,retard", RETARDS.items())
def test_retards_sans_futur(gold, colonne, retard):
    """tm_j_moins_k du jour J doit être égal à tm du jour J-k."""
    tm_par_date = gold.set_index("date")["tm"]
    jours_precedents = gold["date"] - pd.Timedelta(days=retard)
    attendu = tm_par_date.reindex(jours_precedents).to_numpy()
    comparables = ~pd.isna(attendu)
    assert comparables.any(), "Aucune ligne comparable"
    assert (gold.loc[comparables, colonne].to_numpy() == attendu[comparables]).all()
