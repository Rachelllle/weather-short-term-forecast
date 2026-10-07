"""Bronze -> Silver en PySpark : une table propre, une ligne par station et par jour."""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def racine_projet() -> Path:
    """Remonte depuis ce script jusqu'au dossier du projet (celui qui contient data/bronze)."""
    for dossier in Path(__file__).resolve().parents:
        if (dossier / "data" / "bronze").is_dir():
            return dossier
    raise SystemExit("Dossier data/bronze introuvable : lance d'abord l'ingestion Bronze.")


# Chemins calculés depuis l'emplacement du script : peu importe d'où tu le lances
RACINE = racine_projet()
DOSSIER_BRONZE = (RACINE / "data" / "bronze").as_posix()
DOSSIER_SILVER = (RACINE / "data" / "silver" / "journaliere").as_posix()  # dossier de fichiers Parquet
TEMPERATURES = ["tn", "tx", "tm", "tntxm"]

# Schéma attendu des fichiers Bronze (on ne lit que ce dont on a besoin)
SCHEMA = """
    meta_ingestion STRUCT<ingere_le: STRING>,
    reponse_api STRUCT<mesures: ARRAY<STRUCT<
        ic_id: STRING, mfid: STRING, date: STRING,
        tn: DOUBLE, tx: DOUBLE, tm: DOUBLE, tntxm: DOUBLE, rr: DOUBLE,
        n_obs: BIGINT, n_obs_rejetees: BIGINT, statut_extremes: STRING>>>
"""


def lire_bronze(spark: SparkSession) -> DataFrame:
    """Lit tous les fichiers JSON du Bronze et met les mesures à plat."""
    brut = (
        spark.read.schema(SCHEMA)
        .option("multiLine", True)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.json")
        .json(DOSSIER_BRONZE)
        .withColumn("fichier_source", F.input_file_name())
    )
    # explode ignore les fichiers vides ou d'un autre format (mesures absentes)
    return brut.select(
        F.explode("reponse_api.mesures").alias("m"),
        F.col("meta_ingestion.ingere_le").alias("ingere_le"),
        "fichier_source",
    ).select("m.*", "ingere_le", "fichier_source")


def nettoyer(df: DataFrame) -> DataFrame:
    # 1. Types : la date devient une vraie date, on écarte les lignes sans date
    df = df.withColumn("date", F.to_date("date")).filter(
        F.col("date").isNotNull() & F.col("ic_id").isNotNull()
    )

    # 2. Doublons : on garde la ligne ingérée le plus récemment
    plus_recent = Window.partitionBy("ic_id", "date").orderBy(F.col("ingere_le").desc_nulls_last())
    df = df.withColumn("rang", F.row_number().over(plus_recent)).filter("rang = 1").drop("rang")

    # 3. Valeurs impossibles : on les vide au lieu de supprimer la ligne
    temp_ko = (F.col("tn") > F.col("tx")) | (F.col("tn") < -50) | (F.col("tx") > 60)
    df = df.withColumn("temp_ko", F.coalesce(temp_ko, F.lit(False)))
    for colonne in TEMPERATURES:
        df = df.withColumn(colonne, F.when(F.col("temp_ko"), None).otherwise(F.col(colonne)))
    df = df.drop("temp_ko").withColumn("rr", F.when(F.col("rr") < 0, None).otherwise(F.col("rr")))

    # 4. Calendrier complet : une ligne par jour, même les jours sans mesure
    bornes = df.groupBy("ic_id").agg(
        F.min("date").alias("debut"),
        F.max("date").alias("fin"),
        F.first("mfid", ignorenulls=True).alias("mfid_station"),
    )
    calendrier = bornes.select(
        "ic_id",
        "mfid_station",
        F.explode(F.sequence("debut", "fin", F.expr("interval 1 day"))).alias("date"),
    )
    df = (
        calendrier.join(df, ["ic_id", "date"], "left")
        .withColumn("mfid", F.coalesce("mfid", "mfid_station"))
        .withColumn("donnee_manquante", F.col("tm").isNull())
    )
    return df.select(
        "ic_id", "mfid", "date", *TEMPERATURES, "rr", "n_obs", "n_obs_rejetees",
        "statut_extremes", "donnee_manquante", "ingere_le", "fichier_source",
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("silver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # petit volume : inutile d'en avoir 200
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    brut = lire_bronze(spark).cache()
    lues = brut.count()
    if lues == 0:
        print(f"Aucune mesure trouvée dans {DOSSIER_BRONZE}")
        spark.stop()
        return

    silver = nettoyer(brut).cache()
    # Bilan par station : utile pour vérifier chaque station ajoutée au Bronze
    stations = (
        silver.groupBy("ic_id")
        .agg(
            F.count("*").alias("jours"),
            F.count("fichier_source").alias("lignes_gardees"),
            F.sum(F.col("donnee_manquante").cast("int")).alias("sans_temperature"),
            F.min("date").alias("debut"),
            F.max("date").alias("fin"),
        )
        .orderBy("ic_id")
        .collect()
    )

    silver.orderBy("ic_id", "date").write.mode("overwrite").parquet(DOSSIER_SILVER)

    gardees = sum(s["lignes_gardees"] for s in stations)
    print(f"Lignes lues dans le Bronze : {lues}")
    print(f"Lignes écartées (doublons, date absente) : {lues - gardees}")
    print(f"Stations dans le Silver : {len(stations)}")
    for s in stations:
        print(f"  {s['ic_id']} : {s['jours']} jours du {s['debut']} au {s['fin']}, "
              f"dont {s['sans_temperature']} sans température")
    print(f"Silver écrit : {DOSSIER_SILVER}")
    spark.stop()


if __name__ == "__main__":
    main()