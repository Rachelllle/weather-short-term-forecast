# Weather short-term forecast

Pipeline de données météo en architecture médaillon (Bronze, Silver, Gold), construit pour prévoir la température à court terme à partir des observations d'une station.

## Source des données et licence

| Élément | Valeur |
| --- | --- |
| Source | Portail de données ouvertes Infoclimat (`https://portail.chom.engineering`) |
| Point d'accès utilisé | `https://climato.chom.engineering/v2/journaliere` |
| Licence | Licence Ouverte (Etalab) 2.0 + valeur ajoutée Infoclimat (opendata IC) |
| Attribution | Météo-France / Infoclimat |
| Station | `07486` (numéro Météo-France `38384001`) |
| Période | Du 2015-01-01 au 2026-09-29 |
| Date de récupération | 5 octobre 2026 |
| Données personnelles | Aucune : mesures météo par station, sans sujet RGPD |

La Licence Ouverte autorise la réutilisation des données à condition de citer leur source et leur date de mise à jour. Mention à reprendre dans tout rendu qui utilise ces données :

> Données : Météo-France / Infoclimat, Licence Ouverte (Etalab) 2.0, récupérées le 5 octobre 2026.

## Architecture

| Couche | Contenu | Emplacement | Script |
| --- | --- | --- | --- |
| Bronze | Réponses brutes de l'API, en JSON | `data/bronze/v2_journaliere/` | `src/ingestion/bronze.py` |
| Silver | Table nettoyée, une ligne par station et par jour, en Parquet | `data/silver/journaliere/` | `src/transformation/silver.py` |

La couche Gold et le modèle de prévision sont en cours de construction.

Le détail des champs, des types et des règles de nettoyage est dans [docs/dictionnaire_donnees.md](docs/dictionnaire_donnees.md).

## Structure du dépôt

```
weather-short-term-forecast/
├── data/                      # données, non versionnées
│   ├── bronze/
│   └── silver/
├── docs/
│   └── dictionnaire_donnees.md
├── src/
│   ├── ingestion/
│   │   └── bronze.py
│   └── transformation/
│       ├── silver.py
│       └── voir_silver.py
├── .gitignore
└── README.md
```

## Lancer le pipeline

Prérequis : Python 3, Java 17 ou 21 (exigé par PySpark 4), puis :

```
pip install pyspark requests
```

Depuis la racine du projet :

```
python src/ingestion/bronze.py              # API -> Bronze
python src/transformation/silver.py         # Bronze -> Silver
python src/transformation/voir_silver.py    # aperçu de la table Silver
```

`bronze.py` doit être lancé depuis la racine, car il écrit dans `data/bronze` par rapport au dossier courant.

## Règles de gouvernance

- **Bronze immuable** : un fichier écrit n'est jamais modifié. Une nouvelle ingestion crée un nouveau fichier horodaté.
- **Silver reconstructible** : la table est recréée en entier à partir du Bronze à chaque exécution.
- **Traçabilité** : chaque ligne du Silver garde sa date d'ingestion (`ingere_le`) et son fichier d'origine (`fichier_source`).
- **Données hors Git** : le dossier `data/` n'est pas versionné, seuls le code et la documentation le sont.