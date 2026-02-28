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
# Load the success_client table into a DataFrame
success_spark = spark.read.format("delta").load(bronze_success_spark_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to success_job table in Bronze Lakehouse
bronze_success_job_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_bronze.Lakehouse/Tables/dbo/success_job"
# Load the sucess_job table into a Dataframe
success_job = spark.read.format("delta").load(bronze_success_job_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to success_sales table in Bronze Lakehouse
bronze_success_sales_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_bronze.Lakehouse/Tables/dbo/success_sales"
# Load the success_sales table into a Dataframe
success_sales = spark.read.format("delta").load(bronze_success_sales_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to success_participant_list table in Bronze Lakehouse
bronze_success_participant_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_bronze.Lakehouse/Tables/dbo/success_participant_list"
# Load the success_participant_list table into a Dataframe
success_participant = spark.read.format("delta").load(bronze_success_participant_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to success_hired_employee table in Bronze Lakehouse
bronze_success_hired_employee_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_bronze.Lakehouse/Tables/dbo/success_hired_employee"
# Load the success_hired_employee table into a Dataframe
success_hired = spark.read.format("delta").load(bronze_success_hired_employee_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to success_fired_employee table in Bronze Lakehouse
bronze_success_fired_employee_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_bronze.Lakehouse/Tables/dbo/success_fired_employee"
# Load the success_fired_employee table into a Dataframe
success_fired = spark.read.format("delta").load(bronze_success_fired_employee_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to success_fired_employee table in Bronze Lakehouse
bronze_success_activity_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_bronze.Lakehouse/Tables/dbo/success_activity_monitoring"
# Load the success_fired_employee table into a Dataframe
success_activity = spark.read.format("delta").load(bronze_success_activity_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(success_activity)

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

# clean and process the dataset success_job
job_updated = success_job.withColumn("nbre_employe_embauche", when(col("nbre_employe_embauche").isNull(), 0)\
                         .otherwise(col("nbre_employe_embauche")))\
                         .withColumn("nombre_employe_licencie", when(col("nombre_employe_licencie").isNull(), 0)\
                         .otherwise(col("nombre_employe_licencie")))\
                         .withColumn("date_collecte", to_date(col("date_collecte"), "M/d/yyyy"))\
                         .withColumn("day", dayofmonth(col("date_collecte")))\
                         .withColumn("month", month(col("date_collecte")))\
                         .withColumn("quarter", quarter(col("date_collecte")))\
                         .withColumn("year", year(col("date_collecte")))\
                         .withColumnRenamed("nbre_employe_embauche", "total_hired_employee")\
                         .withColumnRenamed("nombre_employe_licencie", "total_fired_employee")\
                         .withColumnRenamed("date_collecte", "collecting_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# clean and process the dataset success_sales
sales_updated = success_sales.withColumn("domestic_sale", when(col("domestic_sale").isNull(), 0)\
                         .otherwise(col("domestic_sale")))\
                         .withColumn("sales_reporting_date", to_date(col("sales_reporting_date"), "M/d/yyyy"))\
                         .withColumn("day_sales_reporting", dayofmonth(col("sales_reporting_date")))\
                         .withColumn("month_sales_reporting", month(col("sales_reporting_date")))\
                         .withColumn("quarter_sales_reporting", quarter(col("sales_reporting_date")))\
                         .withColumn("year_sales_reporting", year(col("sales_reporting_date")))\
                         .withColumn("international_sale", when(col("international_sale").isNull(), 0)\
                         .otherwise(col("international_sale")))
            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean and process the dataset success_participant_list
participant_updated = success_participant.withColumnRenamed("activity_key", "activity_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean and process the dataset success_hired_employee
hiring_updated = success_hired.replace({
    "Oui" : "oui",
    "Non" : "non"
 }, subset=["contrat_temps_plein"])

hiring_updated = hiring_updated.replace({
    "Oui" : "oui",
    "Non" : "non"
 }, subset=["est_assure"])

hiring_updated = hiring_updated.replace({
    "Oui" : "oui",
    "Non" : "non"
 }, subset=["grace_support_meda"])

hiring_updated = hiring_updated.replace({
    "Oui" : "oui",
    "Non" : "non"
 }, subset=["est_decent_work"])

hiring_updated = hiring_updated.replace({
    "oui" : "yes",
    "non" : "no"
 }, subset=["contrat_temps_plein"])

hiring_updated = hiring_updated.replace({
    "oui" : "yes",
    "non" : "no"
 }, subset=["est_assure"])

hiring_updated = hiring_updated.replace({
    "oui" : "yes",
    "non" : "no"
 }, subset=["grace_support_meda"])

hiring_updated = hiring_updated.replace({
    "oui" : "yes",
    "non" : "no"
 }, subset=["est_decent_work"])

hiring_updated = hiring_updated.withColumn("duree_contrat", round(col("duree_contrat").cast("int")))\
                               .withColumnRenamed("sexe", "sex")\
                               .withColumnRenamed("contrat_temps_plein", "is_full_time")\
                               .withColumnRenamed("est_assure", "have_insurance")\
                               .withColumnRenamed("grace_support_meda", "thanks_to_project_support")\
                               .withColumnRenamed("est_decent_work", "is_decent_work")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean and process the dataset success_fired_employee
firing_updated = success_fired.withColumn("age", when(col("age") == 1518, 17)\
                              .when(col("age")== 1935, 27)\
                              .when(col("age")== 3665, 51)\
                              .otherwise(col("age")))\
                              .withColumnRenamed("sexe", "sex")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(firing_updated)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hiring_updated[["age"]].describe().show()

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
