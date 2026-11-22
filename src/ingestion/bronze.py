"""
Ingestion Bronze : télécharge les données journalières depuis l'API climato
et les stocke telles quelles (JSON brut + métadonnées), sans aucune transformation.

Règles :
  - une requête par station et par année
  - les années consolidées déjà présentes ne sont pas retéléchargées
  - l'année en cours (et l'année précédente jusqu'au 15 janvier) est retéléchargée
    à chaque lancement, car ses données peuvent encore arriver ou être corrigées
  - écriture atomique : un fichier est soit complet, soit absent
  - une réponse tronquée n'est jamais enregistrée
  - réessais seulement pour les erreurs temporaires (réseau, 429, 5xx)
  - une année en échec n'arrête pas les autres ; le script se termine en erreur
    (code 1) si au moins une année a échoué, pour que l'orchestrateur le sache

Usage :
    python src/ingestion/bronze.py                          # station 07486, de 2015 à l'année en cours
    python src/ingestion/bronze.py --stations 07486 --debut 2015 --fin 2026
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

API_URL = "https://climato.chom.engineering/v2/journaliere"


RACINE = Path(__file__).resolve().parents[2]
BRONZE_DIR = RACINE / "data" / "bronze" / "v2_journaliere"

PAUSE_ENTRE_APPELS = 1.0       
NB_ESSAIS = 3
DELAI_TIMEOUT = 30            
JOUR_CONSOLIDATION = (1, 15)   
EN_TETES = {"User-Agent": "ESGI-M2-IABD-projet-architecture-cloud (ingestion Bronze)"}


def maintenant_utc() -> datetime:
    return datetime.now(timezone.utc)


def annee_consolidee(annee: int, maintenant: datetime) -> bool:
    """Vrai si les données de l'année ne bougeront plus (on peut ne plus les retélécharger)."""
    if annee < maintenant.year - 1:
        return True
    if annee == maintenant.year - 1:
        return (maintenant.month, maintenant.day) >= JOUR_CONSOLIDATION
    return False  


def deja_complet(dossier: Path, annee: int) -> bool:
    fichiers = sorted(dossier.glob("ingestion_*.json"))  #y a t'il au moins un fichier json
    if not fichiers:
        return False
    dernier = fichiers[-1]

    mois, jour = JOUR_CONSOLIDATION
    seuil = f"{annee + 1}{mois:02d}{jour:02d}T000000Z"  
    horodatage = dernier.stem.removeprefix("ingestion_")
    if horodatage < seuil:
        return False  

    try:  
        contenu = json.loads(dernier.read_text(encoding="utf-8"))
        return isinstance(contenu["reponse_api"]["mesures"], list)
    except (ValueError, KeyError, TypeError):
        return False  


def erreur_temporaire(erreur: requests.RequestException) -> bool:
    if isinstance(erreur, requests.HTTPError) and erreur.response is not None:
        code = erreur.response.status_code
        return code == 429 or code >= 500
    return True  


def appeler_api(station: str, debut: str, fin: str) -> dict:
    params = {"station": station, "from": debut, "to": fin}
    for essai in range(1, NB_ESSAIS + 1):
        try:
            reponse = requests.get(API_URL, params=params, headers=EN_TETES, timeout=DELAI_TIMEOUT)
            reponse.raise_for_status()
            return reponse.json()
        except requests.RequestException as erreur:
            if not erreur_temporaire(erreur) or essai == NB_ESSAIS:
                raise
            attente = 2 ** essai
            print(f"  Erreur temporaire ({erreur}), nouvel essai dans {attente} s...")
            time.sleep(attente)


def ecrire_atomique(fichier: Path, contenu: dict) -> None:
    temporaire = fichier.with_suffix(".tmp")
    temporaire.write_text(json.dumps(contenu, ensure_ascii=False), encoding="utf-8")
    temporaire.replace(fichier)


def ingerer_annee(station: str, annee: int) -> str:
    dossier = BRONZE_DIR / f"station={station}" / f"annee={annee}"
    maintenant = maintenant_utc()

    if annee_consolidee(annee, maintenant) and deja_complet(dossier, annee):
        print(f"  {station} {annee} : déjà présent, ignoré")
        return "ignoree"

    debut, fin = f"{annee}-01-01", f"{annee + 1}-01-01"
    contenu = appeler_api(station, debut, fin)

    if contenu.get("tronque"):
        raise RuntimeError("réponse tronquée : données incomplètes, rien n'est enregistré")

    horodatage = maintenant.strftime("%Y%m%dT%H%M%SZ")
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
    ecrire_atomique(fichier, enregistrement)
    print(f"  {station} {annee} : {contenu.get('n', 0)} jours -> {fichier.relative_to(RACINE)}")

    time.sleep(PAUSE_ENTRE_APPELS)  
    return "telechargee"


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingestion Bronze")
    parser.add_argument("--stations", nargs="+", default=["07486"])
    parser.add_argument("--debut", type=int, default=2015)
    parser.add_argument("--fin", type=int, default=maintenant_utc().year)
    args = parser.parse_args()

    bilan = {"telechargee": 0, "ignoree": 0}
    echecs = []

    for station in args.stations:
        print(f"Station {station}")
        for annee in range(args.debut, args.fin + 1):
            try:
                bilan[ingerer_annee(station, annee)] += 1
            except Exception as erreur:  
                print(f"  {station} {annee} : ÉCHEC ({erreur})")
                echecs.append(f"{station} {annee}")

    print(f"Bilan : {bilan['telechargee']} téléchargée(s), {bilan['ignoree']} ignorée(s), "
          f"{len(echecs)} échec(s)")
    if echecs:
        print("Années en échec : " + ", ".join(echecs))
        return 1  
    return 0


if __name__ == "__main__":
    sys.exit(main())