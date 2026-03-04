# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ad6c2670-8e52-4cb1-a385-669f61c55b3f",
# META       "default_lakehouse_name": "LH_success_silver",
# META       "default_lakehouse_workspace_id": "32baed3b-fac8-4061-9887-3b7530630ba4",
# META       "known_lakehouses": [
# META         {
# META           "id": "ad6c2670-8e52-4cb1-a385-669f61c55b3f"
# META         },
# META         {
# META           "id": "f6edd3ff-48af-414c-b2a8-1b0ced2322dd"
# META         },
# META         {
# META           "id": "ec0b398f-b6d2-42da-af11-ac8f3d497423"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Data processing to generate fact and dimension tables from existing datasets to build a semantic model.
# This notebook describe the process to extract fact tables from each existing datasets stored into the Silver Lakehouse, along with dimension tables. Here are the following steps to executes these tasks:
# * All the datasets will be imported from the silver lakehouse
# * All required variables will be selected from non-normalized datasets to build every new fact table.
# * A primary key will be generated to each fact table. This key is also the foreign key into the dimension table.
# 
# The new fact and dimension tables will be loaded into the gold lakehouse to build the semantic model (Relational database).

# MARKDOWN ********************

# #### Load the modules required to perform operational process tasks.

# CELL ********************

from pyspark.sql import Window
from pyspark.sql.functions import row_number, when, col, isnan, isnull, round, lower
from pyspark.sql.types import DecimalType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1. Processing case of the dataset success_updated by extracting fact and dimension tables.
# * Importing the dataset from the Lakehouse Silver.
# * Extract all fact and dimension tables. 
# * Generate Key primary and foreign one.
# * Remove non-required variables.

# CELL ********************

# Importing the dataset success_updated
success_updated_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_silver.Lakehouse/Tables/dbo/success_updated"

# Load the success_updated table into a DataFrame
success_updated = spark.read.format("delta").load(success_updated_path) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Further process on success_updated dataset to set modalities inconsistencies on required variables.

# CELL ********************

# Modify variables values and change variables type to handle overall inconsistencies and error typing.

success_silver = success_updated.withColumn("business_owner_age", round(col("business_owner_age").cast("int")))\
                                .withColumn("msme_category", when(col("msme_category").isNull(), "Micro")\
                                .otherwise(col("msme_category")))\
                                .withColumn("msme_productive", when(col("msme_productive").isNull(), "no")\
                                .when(lower(col("msme_productive")) == "oui", "yes")\
                                .when(lower(col("msme_productive")) == "non", "no")
                                .otherwise(col("msme_productive")))\
                                .withColumn("industry", when(lower(col("industry")) == "agribusiness", "Agribusiness")\
                                .otherwise(col("industry")))\
                                .withColumn("departement", when(col("departement") == "NIPPES", "Nippes")\
                                .when(col("departement") == "Sud-EST", "Sud-Est")\
                                .when(col("departement").isin("ARTIBONITE","artibonite"), "Artibonite")\
                                .otherwise(col("departement")))\
                                .withColumn("financial_institution", when(col("financial_institution") == "NON", "no")\
                                .otherwise(col("financial_institution")))\
                                .withColumn("business_owner_sex", when(col("business_owner_sex") == "f", "F")\
                                .when(col("business_owner_sex") == "m", "M")\
                                .otherwise(col("business_owner_sex")))\
                                .withColumn("annual_income", when(col("annual_income").rlike(r"^-?\d+(\.\d+)?$"), col("annual_income").cast(DecimalType(10, 2)))\
                                .otherwise(None))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Creation of the date dimension table from success_updated

# CELL ********************

date_success = success_silver.select("registration_date", "day", "month", "quarter", "year").distinct()\
                            .withColumnRenamed("registration_date", "registration_date_id")\
                            .withColumnRenamed("day", "day_success")\
                            .withColumnRenamed("month", "month_success")\
                            .withColumnRenamed("quarter", "quarter_success")\
                            .withColumnRenamed("year", "year_success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Creation of the partner dimension table from success_updated

# CELL ********************

partner_success = success_silver.select("business_partner", "financial_institution").distinct()\
                                 .withColumn("partener_id", row_number().over(Window.orderBy("business_partner", "financial_institution")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Creation of the project intervention area dimension table

# CELL ********************

area_success = success_silver.select("departement").distinct()\
                                 .withColumn("area_id", row_number().over(Window.orderBy("departement")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Creation of industry and MSME category dimension table

# CELL ********************

industry_success = success_silver.select("industry").distinct()\
                                 .withColumn("industry_id", row_number().over(Window.orderBy("industry")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Creation of type of Savings group dimension table

# CELL ********************

savings_success = success_silver.select("savings_group").distinct()\
                                 .withColumn("savings_id", row_number().over(Window.orderBy("savings_group")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Linking all dimension tables to success_silver dataset adding the primary keys.

# CELL ********************

# Join the dimension tables to the original dataframe success_silver
success_silver = success_silver.join(partner_success, ["business_partner", "financial_institution"], "left")\
                               .join(area_success, ["departement"], "left")\
                               .join(industry_success, ["industry"], "left")\
                               .join(savings_success, ["savings_group"], "left" )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Create the fact table from the original dataset success_silver

# CELL ********************

# Select all required variables to create the fact table success_fact
success_fact = success_silver.select("msme_key", "registration_date", "partener_id", "area_id", "industry_id", "savings_id", "business_owner_sex", "business_owner_age", "msme_productive", "msme_category", "annual_income", "employee_number", "loan_amount")\
                             .withColumnRenamed("registration_date", "registration_date_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Save the fact and dimension tables extracted from the original dataset success_silver into the Lakehouse Gold.

# CELL ********************

# Generate each of path related to every table.
gold_success_fact_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/success_fact"
gold_savings_success_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/savings_success"
gold_industry_success_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/industry_success"
gold_area_success_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/area_success"
gold_partner_success_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/partner_success"
gold_date_success_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/date_success"

# Save the tables into the gold Lakehouse
success_fact.write.format("delta").mode("overwrite").save(gold_success_fact_path)
savings_success.write.format("delta").mode("overwrite").save(gold_savings_success_path)
industry_success.write.format("delta").mode("overwrite").save(gold_industry_success_path)
area_success.write.format("delta").mode("overwrite").save(gold_area_success_path)
partner_success.write.format("delta").mode("overwrite").save(gold_partner_success_path)
date_success.write.format("delta").mode("overwrite").save(gold_date_success_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2. Processing case of the dataset activity_updated by extracting fact and dimension tables.
# * Importing the dataset from the Lakehouse Silver.
# * Extract all fact and dimension tables. 
# * Generate Key primary and foreign one.
# * Remove non-required variables.
