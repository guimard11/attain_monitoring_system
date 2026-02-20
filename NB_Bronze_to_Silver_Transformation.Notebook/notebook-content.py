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
from pyspark.sql.functions import when, col, isnan, isnull,dayofmonth, month, quarter, year, round, to_date

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
success_updated = success_spark.replace({
    "ACME 2": "ACME",
    "ACME 3": "ACME",
    "AIDE 2": "AIDE",
    "CAPOSUD 2": "CAPOSUD",
    "CAPUC 2" : "CAPUC",
    "CEDEL 2": "CEDEL",
}, subset=["partenaire"])

# Replace data value name into the column "institution_financiere"
success_updated = success_updated.replace({
    "Oui": "yes",
    "Non": "no"
}, subset=["institution_financiere"])

# Replace data value name into the column "msme_productive"
success_updated = success_updated.replace({
    "Oui": "yes",
    "Non": "no"
}, subset=["msme_productive"])

# Replace data value name into the column "group_epargne"
success_updated = success_updated.replace({
    "Aucun": "No group",
    "Association": "Association",
    "AVEC": "AVEC group",
    "BD&SME": "BD&SME",
    "MUSO": "MUSO",
    "Solidarite": "Solidarity group"
}, subset=["group_epargne"])

# Replace all values inferior to 10000 and blank value in revenu_annuel
success_updated = success_updated.withColumn("revenu_annuel", when(col("revenu_annuel") < 25000, 0)\
                             .when(col("revenu_annuel").isNull(), 0)\
                             .otherwise(col("revenu_annuel")))\
                             .withColumn("nombre_employe", when(col("nombre_employe").isNull(), 1)\
                             .otherwise(col("nombre_employe")))\
                             .withColumn("nombre_employe", round(col("nombre_employe").cast("int")))\
                             .withColumn("age", when(col("age").isNull(), 18)\
                             .when((col("age")< 18) & (col("age")> 1), 18).otherwise(col("age")))\
                             .withColumn("secteur_activite", when(col("secteur_activite") == "Service", "Services")\
                              .when(col("secteur_activite").isNull(), "Services")\
                              .otherwise(col("secteur_activite")))\
                              .withColumn("date_enregistrement", to_date(col("date_enregistrement"), "M/d/yyyy"))\
                              .withColumn("day", dayofmonth(col("date_enregistrement")))\
                              .withColumn("month", month(col("date_enregistrement")))\
                              .withColumn("quarter", quarter(col("date_enregistrement")))\
                              .withColumn("year", year(col("date_enregistrement")))\
                              .withColumnRenamed("partenaire", "business_partner")\
                              .withColumnRenamed("institution_financiere", "financial_institution")\
                              .withColumnRenamed("date_enregistrement", "registration_date")\
                              .withColumnRenamed("nom_msme", "msme_name")\
                              .withColumnRenamed("revenu_annuel", "annual_income")\
                              .withColumnRenamed("type_msme", "msme_category")\
                              .withColumnRenamed("secteur_activite", "industry")\
                              .withColumnRenamed("nombre_employe", "employee_number")\
                              .withColumnRenamed("group_epargne", "savings_group")\
                              .withColumnRenamed("montant_pret", "loan_amount")\
                              .withColumnRenamed("age", "business_owner_age")\
                              .withColumnRenamed("sex", "business_owner_sex")
                              

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(success_updated)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

col_type = dict(success_updated.dtypes)["year"]
print(col_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

success_updated.select("year").distinct().orderBy("year").show()
#df.select("ma_colonne").distinct().orderBy("ma_colonne").show()


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
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
