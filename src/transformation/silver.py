"""
Règles appliquées :
  - dédoublonnage (station, date) : on garde la ligne téléchargée le plus récemment
  - valeurs impossibles vidées, jamais inventées, et signalées par une colonne
  - colonne tntxm supprimée (car identique à tm)
  - calendrier complet : un jour absent devient une ligne vide, signalée
  - indicateurs de qualité : jour_present, jour_complet,
    temperatures_invalides, pluie_invalide
"""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

N_OBS_MIN = 40                    
TEMP_MIN, TEMP_MAX = -40.0, 50.0  
PLUIE_MAX = 400.0                 
TEMPERATURES = ["tn", "tx", "tm"]


def racine_projet() -> Path:
    for dossier in Path(__file__).resolve().parents:
        if (dossier / "data" / "bronze").is_dir():
            return dossier
    raise SystemExit("Dossier data/bronze introuvable : lancez d'abord src/ingestion/bronze.py")


RACINE = racine_projet()
DOSSIER_BRONZE = (RACINE / "data" / "bronze" / "v2_journaliere").as_posix()
DOSSIER_SILVER = (RACINE / "data" / "silver" / "journaliere").as_posix()

SCHEMA = """
    meta_ingestion STRUCT<ingere_le: STRING>,
    reponse_api STRUCT<mesures: ARRAY<STRUCT<
        ic_id: STRING, mfid: STRING, date: STRING,
        tn: DOUBLE, tx: DOUBLE, tm: DOUBLE, tntxm: DOUBLE, rr: DOUBLE,
        n_obs: BIGINT, n_obs_rejetees: BIGINT, statut_extremes: STRING>>>
"""


def lire_bronze(spark: SparkSession) -> DataFrame:
    """Lit tous les fichiers JSON de la source et met les mesures à plat (une ligne par jour)."""
    brut = (
        spark.read.schema(SCHEMA)
        .option("multiLine", True)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.json")
        .json(DOSSIER_BRONZE)
        .withColumn("chemin", F.input_file_name())
    )
    return brut.select(
        F.explode("reponse_api.mesures").alias("m"),  #ignore les fichiers sans mesures
        F.col("meta_ingestion.ingere_le").alias("ingere_le"),
        #Nom du fichier seulement : le chemin complet change d'une machine à l'autre
        F.element_at(F.split("chemin", "/"), -1).alias("fichier_source"),
    ).select("m.*", "ingere_le", "fichier_source")


def nettoyer(df: DataFrame) -> DataFrame:
    #ic_id devient station, la date devient une vraie date
    df = (
        df.withColumnRenamed("ic_id", "station")
        .withColumn("date", F.to_date("date"))
        .filter(F.col("date").isNotNull() & F.col("station").isNotNull())
        .drop("tntxm", "statut_extremes")  
    )

    # 1 seule ligne par (station, date), la plus récemment téléchargée
    plus_recent = Window.partitionBy("station", "date").orderBy(F.col("ingere_le").desc_nulls_last())
    df = df.withColumn("rang", F.row_number().over(plus_recent)).filter("rang = 1").drop("rang")

    
    #tm étant calculée à partir de tn et tx, si l'une est fausse, les trois sont vidées
    hors_bornes = F.lit(False)
    for colonne in TEMPERATURES:
        hors_bornes = hors_bornes | F.coalesce(~F.col(colonne).between(TEMP_MIN, TEMP_MAX), F.lit(False))
    incoherent = F.coalesce(F.col("tn") > F.col("tx"), F.lit(False))
    df = df.withColumn("temperatures_invalides", hors_bornes | incoherent)
    for colonne in TEMPERATURES:
        df = df.withColumn(colonne, F.when(F.col("temperatures_invalides"), None).otherwise(F.col(colonne)))

    #Pluie impossible : négative ou supérieure à PLUIE_MAX
    pluie_ko = F.coalesce((F.col("rr") < 0) | (F.col("rr") > PLUIE_MAX), F.lit(False))
    df = df.withColumn("pluie_invalide", pluie_ko).withColumn(
        "rr", F.when(F.col("pluie_invalide"), None).otherwise(F.col("rr"))
    )

    #Calendrier complet : une ligne par jour et par station, même les jours sans mesure
    bornes = df.groupBy("station").agg(
        F.min("date").alias("debut"),
        F.max("date").alias("fin"),
        F.first("mfid", ignorenulls=True).alias("mfid_station"),
    )
    calendrier = bornes.select(
        "station",
        "mfid_station",
        F.explode(F.sequence("debut", "fin", F.expr("interval 1 day"))).alias("date"),
    )
    df = (
        calendrier.join(df, ["station", "date"], "left")
        .withColumn("mfid", F.coalesce("mfid", "mfid_station"))
        #Indicateurs de qualité
        .withColumn("jour_present", F.col("fichier_source").isNotNull())
        .withColumn("jour_complet", F.coalesce(F.col("n_obs") >= N_OBS_MIN, F.lit(False)))
        .withColumn("temperatures_invalides", F.coalesce("temperatures_invalides", F.lit(False)))
        .withColumn("pluie_invalide", F.coalesce("pluie_invalide", F.lit(False)))
    )

    return df.select(
        "station", "mfid", "date", *TEMPERATURES, "rr", "n_obs", "n_obs_rejetees",
        "jour_present", "jour_complet", "temperatures_invalides", "pluie_invalide",
        "ingere_le", "fichier_source",
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("silver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  
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

    #Bilan par station
    stations = (
        silver.groupBy("station")
        .agg(
            F.count("*").alias("jours"),
            F.sum(F.col("jour_present").cast("int")).alias("presents"),
            F.sum((F.col("jour_present") & ~F.col("jour_complet")).cast("int")).alias("incomplets"),
            F.sum(F.col("temperatures_invalides").cast("int")).alias("temp_invalides"),
            F.sum(F.col("pluie_invalide").cast("int")).alias("pluie_invalides"),
            F.min("date").alias("debut"),
            F.max("date").alias("fin"),
        )
        .orderBy("station")
        .collect()
    )

    
    silver.orderBy("station", "date").coalesce(1).write.mode("overwrite").parquet(DOSSIER_SILVER)

    gardees = sum(s["presents"] for s in stations)
    print(f"Lignes lues dans le Bronze           : {lues}")
    print(f"Lignes écartées (doublons, sans date) : {lues - gardees}")
    print(f"Stations dans le Silver               : {len(stations)}")
    for s in stations:
        print(f"  {s['station']} : {s['jours']} jours du {s['debut']} au {s['fin']}")
        print(f"      jours manquants        : {s['jours'] - s['presents']}")
        print(f"      jours incomplets       : {s['incomplets']}")
        print(f"      températures invalides : {s['temp_invalides']}")
        print(f"      pluies invalides       : {s['pluie_invalides']}")
    print(f"Silver écrit : {DOSSIER_SILVER}")
    spark.stop()


if __name__ == "__main__":
    main()