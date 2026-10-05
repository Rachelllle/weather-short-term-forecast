"""
Ingestion Bronze : télécharge les données journalières depuis l'API climato
et les stocke telles quelles (JSON brut + métadonnées), sans aucune transformation

Usage :
    python src/ingestion/bronze.py --stations 07486 --debut 2015 --fin 2024
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

API_URL = "https://climato.chom.engineering/v2/journaliere"
BRONZE_DIR = Path("data/bronze/v2_journaliere")
PAUSE_ENTRE_APPELS = 1.0  
NB_ESSAIS = 3


def appeler_api(station: str, debut: str, fin: str) -> dict:
    params = {"station": station, "from": debut, "to": fin}
    for essai in range(1, NB_ESSAIS + 1):
        try:
            reponse = requests.get(API_URL, params=params, timeout=30)
            reponse.raise_for_status()
            return reponse.json()
        except requests.RequestException as erreur:
            if essai == NB_ESSAIS:
                raise
            attente = 2 ** essai
            print(f"  Erreur ({erreur}), nouvel essai dans {attente} s...")
            time.sleep(attente)


def ingerer_annee(station: str, annee: int) -> None:
    dossier = BRONZE_DIR / f"station={station}" / f"annee={annee}"
    annee_terminee = annee < datetime.now().year
    if annee_terminee and dossier.exists() and any(dossier.glob("*.json")):
        print(f"  {station} {annee} : déjà présent, ignoré")
        return

    debut, fin = f"{annee}-01-01", f"{annee + 1}-01-01"
    contenu = appeler_api(station, debut, fin)

    if contenu.get("tronque"):
        print(f"  ATTENTION : réponse tronquée pour {station} {annee}")

    horodatage = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    enregistrement = {
        "meta_ingestion": {
            "source": API_URL,
            "parametres": {"station": station, "from": debut, "to": fin},
            "ingere_le": horodatage,
        },
        "reponse_api": contenu,  
    }

    dossier.mkdir(parents=True, exist_ok=True)
    fichier = dossier / f"ingestion_{horodatage}.json"
    fichier.write_text(json.dumps(enregistrement, ensure_ascii=False), encoding="utf-8")
    print(f"  {station} {annee} : {contenu.get('n', 0)} jours -> {fichier}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion Bronze")
    parser.add_argument("--stations", nargs="+", default=["07486"])
    parser.add_argument("--debut", type=int, default=2015)
    parser.add_argument("--fin", type=int, default=2024)
    args = parser.parse_args()

    for station in args.stations:
        print(f"Station {station}")
        for annee in range(args.debut, args.fin + 1):
            ingerer_annee(station, annee)
            time.sleep(PAUSE_ENTRE_APPELS)


if __name__ == "__main__":
    main()