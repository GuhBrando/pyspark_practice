from re import X
import findspark
from numpy import where
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

spark = SparkSession\
        .builder\
        .appName('teste')\
        .config("spark.master", "local")\
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
        .getOrCreate()

def openFile(fileName):
    currentLocation = os.getcwd()
    FILE = r"{}\pyspark_practice\spark-essentials-4.3-spark-sql-exercises\src\main\resources\data\{}".format(currentLocation, fileName)
    df = spark.read.option("inferSchema","true").json(FILE)
    return df

def main():
    dfMovies = openFile("movies.json")

    # Datas
    moviesWithReleaseDates = dfMovies.select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").alias("Actual_Release"))
    moviesWithReleaseDates = moviesWithReleaseDates\
        .withColumn("Today", current_date())\
        .withColumn("Right_Now", current_timestamp())\
        .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)
    
    moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull())

    # Estruturas 
    # Operação com Coluna
    dfMovies.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).alias("Profit"))\
        .select(col("Title"), col("Profit").getField("US_Gross").alias("US_Profit"))
    
    # Com expressão de string
    dfMovies.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")\
        .selectExpr("Title", "Profit.US_Gross")

    # Arrays
    moviesWithWords = dfMovies.select(col("Title"), split(col("Title"), " |,").alias("Title_Words")) # Array de string
    moviesWithWords.select(
        col("Title"),
        expr("Title_Words[0]"),
        size(col("Title_Words")),
        array_contains(col("Title_Words"), "Love")
    )


    ## Lidando com Nulls
    # Selecionar valor nao nulo entre 2 colunas
    dfMovies.select(
        col("Title"),
        col("Rotten_Tomatoes_Rating"),
        col("IMDB_Rating"),
        coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
    )

    # Checar nulos
    dfMovies.select("*").where(col("Rotten_Tomatoes_Rating").isNull())

    # Ordernar baseado nos nulos
    dfMovies.orderBy(col("IMDB_Rating").desc_nulls_last())

    # Remover Nulos
    dfMovies.select("Title", "IMDB_Rating").na.drop()

    # Replace em nulos
    dfMovies.na.fill(0, ["IMDB_Rating", "Rotten_Tomatoes_Rating"])
    dfMovies.na.fill({"IMDB_Rating": 0,
    "Rotten_Tomatoes_Rating": 10,
    "Director": "Unkown"}
    ).show()

if __name__ == "__main__":
    main()