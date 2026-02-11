# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5bb995e4-9bc1-42e4-b483-d62aadaafc35",
# META       "default_lakehouse_name": "LH_success_bronze",
# META       "default_lakehouse_workspace_id": "6a95caf4-a121-44cd-8a44-294a59cb0fc5",
# META       "known_lakehouses": [
# META         {
# META           "id": "5bb995e4-9bc1-42e4-b483-d62aadaafc35"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Load required module from Pyspark
import requests
import pandas as pd
from datetime import timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to success_spark table in Bronze Lakehouse
bronze_success_spark_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_bronze.Lakehouse/Tables/dbo/success_spark"
# Load the wind_power table into a DataFrame
success_spark = spark.read.format("delta").load(bronze_success_spark_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(success_spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# clean and process the dataset success_spark

# Replace data value name into the column partenaire
success_spark = success_spark.replace({
    "ACME 2": "ACME",
    "ACME 3": "ACME",
    "AIDE 2": "AIDE",
    "CAPOSUD 2": "CAPOSUD",
    "CAPUC 2" : "CAPUC",
    "CEDEL 2": "CEDEL",
}, subset=["partenaire"])

# Replace data value name into the column "institution_financiere"
success_spark = success_spark.replace({
    "Oui": "yes",
    "Non": "no"
}, subset=["institution_financiere"])

#import function when
from pyspark.sql.functions import when, col, isnan, isnull

# Replace all values inferior to 10000 and blank value in revenu_annuel
success_spark = success_spark.withColumn("revenu_annuel", when(col("revenu_annuel") < 25000, 0)\
                              .when(col("revenu_annuel").isNull(), 0)\
                              .otherwise(col("revenu_annuel")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

success_spark.select("revenu_annuel").orderBy("revenu_annuel").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Save the success_client dataset into spark schema
#success_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_bronze.Lakehouse/Tables/dbo/success_spark"
#success_spark.write.format("delta").mode("overwrite").save(success_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
