"""Silver -> Gold en PySpark : jeu d'apprentissage pour la prévision de tm à J+1, J+3 et J+7."""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def racine_projet() -> Path:
    """Remonte depuis ce script jusqu'au dossier du projet (celui qui contient data/silver)."""
    for dossier in Path(__file__).resolve().parents:
        if (dossier / "data" / "silver").is_dir():
            return dossier
    raise SystemExit("Dossier data/silver introuvable : lance d'abord silver.py.")


# Chemins calculés depuis l'emplacement du script : peu importe d'où tu le lances
RACINE = racine_projet()
DOSSIER_SILVER = (RACINE / "data" / "silver" / "journaliere").as_posix()
DOSSIER_GOLD = (RACINE / "data" / "gold" / "jeu_apprentissage").as_posix()

HORIZONS = [1, 3, 7]          # jours de prévision
RETARDS_TM = [1, 2, 3, 7]     # tm à J-1, J-2, J-3, J-7
FENETRES_TM = [3, 7]          # moyennes glissantes de tm (jours)

# Découpage chronologique par année du jour J
ANNEES_SPLIT = {
    "train": (2015, 2023),
    "validation": (2024, 2024),
    "test": (2025, 2025),         # verrouillé : évaluation finale uniquement
    "test_2026": (2026, 2026),    # second jeu de test
}
ANNEES_NORMALE = ANNEES_SPLIT["train"]  # la normale n'utilise que l'entraînement


def colonne_split(col_date: str) -> F.Column:
    """Renvoie le nom de la période (train, validation, test, test_2026) d'une date."""
    annee = F.year(col_date)
    expr = F.lit(None).cast("string")
    for nom, (debut, fin) in reversed(list(ANNEES_SPLIT.items())):
        expr = F.when(annee.between(debut, fin), nom).otherwise(expr)
    return expr


def lire_silver(spark: SparkSession) -> DataFrame:
    """Lit le Silver. Les jours partiels sont vidés pour ne pas fausser les variables."""
    df = spark.read.parquet(DOSSIER_SILVER).withColumn("date", F.to_date("date"))
    partiel = F.coalesce(F.col("statut_extremes") == "partiel", F.lit(False))
    for colonne in ["tn", "tx", "tm", "rr"]:
        df = df.withColumn(colonne, F.when(partiel, None).otherwise(F.col(colonne)))
    return df.select("ic_id", "date", "tn", "tx", "tm", "rr")


def calculer_normale(df: DataFrame) -> DataFrame:
    """Normale saisonnière : moyenne de tm par station et par jour de l'année (MM-dd), sur l'entraînement."""
    debut, fin = ANNEES_NORMALE
    return (
        df.filter(F.year("date").between(debut, fin))
        .groupBy("ic_id", F.date_format("date", "MM-dd").alias("jour_annee"))
        .agg(F.avg("tm").alias("normale_tm"))
    )


def construire_gold(df: DataFrame, normale: DataFrame) -> DataFrame:
    # Le Silver a un calendrier complet : un décalage d'une ligne = un jour
    par_station = Window.partitionBy("ic_id").orderBy("date")

    # 1. Variables d'entrée : uniquement J et le passé
    for k in RETARDS_TM:
        df = df.withColumn(f"tm_j_moins_{k}", F.lag("tm", k).over(par_station))
    for n in FENETRES_TM:
        fenetre = par_station.rowsBetween(-(n - 1), 0)
        complete = F.count("tm").over(fenetre) == n  # on exige les n jours renseignés
        df = df.withColumn(f"tm_moy_{n}j", F.when(complete, F.avg("tm").over(fenetre)))
    angle = 2 * 3.141592653589793 * F.dayofyear("date") / 365.25
    df = df.withColumn("jour_sin", F.sin(angle)).withColumn("jour_cos", F.cos(angle))

    # 2. Cibles et normale saisonnière de chaque date cible
    for h in HORIZONS:
        df = (
            df.withColumn(f"cible_tm_j{h}", F.lead("tm", h).over(par_station))
            .withColumn("jour_annee", F.date_format(F.date_add("date", h), "MM-dd"))
            .join(normale, ["ic_id", "jour_annee"], "left")
            .withColumnRenamed("normale_tm", f"ref_normale_j{h}")
            .drop("jour_annee")
        )

    # 3. Référence persistance : la valeur future est égale à tm du jour J
    df = df.withColumn("ref_persistance", F.col("tm"))

    # 4. Découpage : on retire les lignes dont la cible J+7 tombe dans la période suivante
    h_max = max(HORIZONS)
    df = (
        df.withColumn("split", colonne_split("date"))
        .withColumn("split_cible", colonne_split(F.date_add("date", h_max)))
        .filter(F.col("split").isNotNull() & (F.col("split") == F.col("split_cible")))
        .drop("split_cible")
    )

    # 5. Lignes exploitables : toutes les variables, cibles et références renseignées
    variables = (
        ["tm", "tn", "tx", "rr"]
        + [f"tm_j_moins_{k}" for k in RETARDS_TM]
        + [f"tm_moy_{n}j" for n in FENETRES_TM]
        + ["jour_sin", "jour_cos"]
    )
    cibles = [f"cible_tm_j{h}" for h in HORIZONS]
    references = ["ref_persistance"] + [f"ref_normale_j{h}" for h in HORIZONS]
    df = df.dropna(subset=variables + cibles + references)

    return df.select(
        "ic_id", "date", "split", *variables, *cibles, *references,
        F.current_timestamp().alias("genere_le"),
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("gold")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # petit volume
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = lire_silver(spark).cache()
    if silver.count() == 0:
        print(f"Aucune ligne trouvée dans {DOSSIER_SILVER}")
        spark.stop()
        return

    gold = construire_gold(silver, calculer_normale(silver)).cache()
    bilan = (
        gold.groupBy("split")
        .agg(F.count("*").alias("lignes"), F.min("date").alias("debut"), F.max("date").alias("fin"))
        .orderBy("debut")
        .collect()
    )

    gold.orderBy("ic_id", "date").write.mode("overwrite").parquet(DOSSIER_GOLD)

    print(f"Stations dans le Gold : {gold.select('ic_id').distinct().count()}")
    for b in bilan:
        print(f"  {b['split']:<10} : {b['lignes']} lignes du {b['debut']} au {b['fin']}")
    print(f"Gold écrit : {DOSSIER_GOLD}")
    spark.stop()


if __name__ == "__main__":
    main()