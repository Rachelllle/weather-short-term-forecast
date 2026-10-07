"""Affiche un aperçu de la table Silver."""
from pathlib import Path

from pyspark.sql import SparkSession

# Remonte depuis ce script jusqu'au dossier du projet (celui qui contient data/silver)
RACINE = next((d for d in Path(__file__).resolve().parents if (d / "data" / "silver").is_dir()), None)
if RACINE is None:
    raise SystemExit("Dossier data/silver introuvable : lance d'abord silver.py.")

spark = SparkSession.builder.appName("voir_silver").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet((RACINE / "data" / "silver" / "journaliere").as_posix())
df.show(5)
df.printSchema()
print("Nombre de lignes :", df.count())

spark.stop()