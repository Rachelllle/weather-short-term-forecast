# Dictionnaire de données — projet médaillon météo

Dernière mise à jour : 2026-10-07

## Source et licence

Les données viennent du portail de données ouvertes Infoclimat et sont réutilisables sous Licence Ouverte (Etalab) 2.0, avec l'attribution « Météo-France / Infoclimat ».

| Élément | Valeur |
| --- | --- |
| Point d'accès | `https://climato.chom.engineering/v2/journaliere` |
| Paramètres d'appel | `station`, `from`, `to` (la date de fin est exclue) |
| Licence | Licence Ouverte (Etalab) 2.0 + valeur ajoutée Infoclimat (opendata IC) |
| Attribution à citer | Météo-France / Infoclimat |
| Données personnelles | Aucune : ce sont des mesures météo par station, sans sujet RGPD |
| Station couverte | `07486` (numéro Météo-France `38384001`) |
| Période couverte | Du 2015-01-01 au 2026-09-29 |

## Couche Bronze

Le Bronze conserve la réponse brute de l'API, sans aucune modification, dans un fichier JSON par station, par année et par ingestion.

| Élément | Valeur |
| --- | --- |
| Emplacement | `data/bronze/v2_journaliere/station=<ic_id>/annee=<AAAA>/ingestion_<horodatage>.json` |
| Produit par | `src/ingestion/bronze.py` |
| Format | JSON, un objet par fichier |
| Règle | Un fichier écrit n'est jamais modifié. Une nouvelle ingestion crée un nouveau fichier horodaté. |
| Volume actuel | 12 fichiers (2015 à 2026), 4 290 mesures journalières |

Chaque fichier contient deux blocs : `meta_ingestion`, ajouté par le script, et `reponse_api`, la réponse de l'API telle quelle.

### Enveloppe du fichier

| Champ | Type | Description | Exemple |
| --- | --- | --- | --- |
| `meta_ingestion.source` | texte | Adresse de l'API appelée | `https://climato.chom.engineering/v2/journaliere` |
| `meta_ingestion.parametres` | objet | Paramètres de l'appel : `station`, `from`, `to` | `07486`, `2015-01-01`, `2016-01-01` |
| `meta_ingestion.ingere_le` | texte | Date et heure UTC de l'ingestion, au format AAAAMMJJTHHMMSSZ | `20261005T142113Z` |
| `reponse_api.station.query` | texte | Station demandée | `07486` |
| `reponse_api.licence` | texte | Licence des données | Licence Ouverte (Etalab) 2.0 + valeur ajoutée Infoclimat (opendata IC) |
| `reponse_api.attribution` | texte | Mention de source à citer | Météo-France / Infoclimat |
| `reponse_api.n` | entier | Nombre de mesures renvoyées | 365 |
| `reponse_api.plafond` | entier | Nombre maximal de mesures par appel | 20000 |
| `reponse_api.tronque` | booléen | Vrai si la réponse a été coupée au plafond | false |
| `reponse_api.fenetre` | objet | Premier et dernier jour réellement couverts (`debut`, `fin`) | `2015-01-01`, `2015-12-31` |
| `reponse_api.suite` | à confirmer | Rôle à confirmer dans la documentation de l'API | null |
| `reponse_api.temps_ms` | nombre | Durée de réponse de l'API, en millisecondes | 1024.7 |
| `reponse_api.mesures` | liste | Mesures journalières, décrites dans le tableau suivant | 365 éléments |

Les descriptions de `plafond` et `tronque` sont déduites du nom des champs et restent à confirmer dans la documentation de l'API.

### Mesures journalières (`reponse_api.mesures`)

| Champ | Type | Description | Exemple |
| --- | --- | --- | --- |
| `ic_id` | texte | Identifiant Infoclimat de la station | `07486` |
| `mfid` | texte | Numéro Météo-France de la station | `38384001` |
| `date` | texte | Jour de la mesure, au format AAAA-MM-JJ | `2015-01-01` |
| `tn` | nombre | Température minimale du jour, en °C | -4.4 |
| `tx` | nombre | Température maximale du jour, en °C | 2.0 |
| `tm` | nombre | Température moyenne du jour, en °C | -1.2 |
| `tntxm` | nombre | Moyenne de `tn` et `tx`, en °C | -1.2 |
| `rr` | nombre | Cumul de précipitations du jour, en mm | 0.0 |
| `n_obs` | entier | Nombre d'observations utilisées pour le jour | 48 |
| `n_obs_rejetees` | entier | Nombre d'observations écartées par le contrôle qualité de l'API | 0 |
| `statut_extremes` | texte | Statut des extrêmes fourni par l'API | `quotidien` |

Dans le fichier 2015, `tm` et `tntxm` ont la même valeur chaque jour.

## Couche Silver

Le Silver est une table Parquet nettoyée, avec une ligne par station et par jour, reconstruite en entier à partir du Bronze à chaque exécution.

| Élément | Valeur |
| --- | --- |
| Emplacement | `data/silver/journaliere/` (dossier de fichiers Parquet) |
| Produit par | `src/transformation/silver.py` (PySpark) |
| Grain | Une ligne par station et par jour |
| Clé | `ic_id` + `date` |
| Mode d'écriture | Remplacement complet à chaque exécution |
| Volume actuel | 4 290 lignes, 1 station, du 2015-01-01 au 2026-09-29 |

### Colonnes

| Colonne | Type | Description | Origine |
| --- | --- | --- | --- |
| `ic_id` | string | Identifiant Infoclimat de la station | Bronze |
| `mfid` | string | Numéro Météo-France de la station | Bronze |
| `date` | date | Jour de la mesure | Bronze, texte converti en date |
| `tn` | double | Température minimale du jour, en °C | Bronze |
| `tx` | double | Température maximale du jour, en °C | Bronze |
| `tm` | double | Température moyenne du jour, en °C | Bronze |
| `tntxm` | double | Moyenne de `tn` et `tx`, en °C | Bronze |
| `rr` | double | Cumul de précipitations du jour, en mm | Bronze |
| `n_obs` | long | Nombre d'observations utilisées pour le jour | Bronze |
| `n_obs_rejetees` | long | Nombre d'observations écartées par le contrôle qualité de l'API | Bronze |
| `statut_extremes` | string | Statut des extrêmes fourni par l'API | Bronze |
| `donnee_manquante` | boolean | Vrai quand `tm` est vide ce jour-là | Calculée dans le Silver |
| `ingere_le` | string | Date et heure UTC d'ingestion du fichier d'origine | `meta_ingestion.ingere_le` du Bronze |
| `fichier_source` | string | Chemin du fichier Bronze d'où vient la ligne | Ajoutée à la lecture du Bronze |

### Règles de nettoyage

| Cas | Traitement |
| --- | --- |
| Ligne sans `date` ou sans `ic_id` | La ligne est écartée |
| Même station et même jour présents plusieurs fois | Seule la ligne dont `ingere_le` est le plus récent est gardée |
| `tn` supérieur à `tx`, `tn` inférieur à -50 °C ou `tx` supérieur à 60 °C | `tn`, `tx`, `tm` et `tntxm` sont vidés, la ligne est gardée |
| `rr` négatif | `rr` est vidé, la ligne est gardée |
| Jour absent entre le premier et le dernier jour d'une station | Une ligne est ajoutée avec des mesures vides et `donnee_manquante` à vrai |

## Traçabilité

Chaque ligne du Silver permet de remonter jusqu'à l'appel d'API qui l'a produite.

1. Dans le Silver, la colonne `fichier_source` donne le chemin du fichier Bronze d'origine.
2. Dans ce fichier, `meta_ingestion` donne l'adresse appelée, les paramètres et la date d'ingestion.
3. Avec ces paramètres, le même appel peut être relancé pour comparer la valeur avec la source.

Les lignes ajoutées pour compléter le calendrier n'ont pas de fichier d'origine : leurs colonnes `fichier_source` et `ingere_le` sont vides.
