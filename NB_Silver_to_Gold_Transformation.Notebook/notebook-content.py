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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1. Process case of the dataset success_updated.
# * Importing the dataset from the silver lakehouse.
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

# CELL ********************

display(success_updated)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Creation of the date dimension table from success_updated

# CELL ********************

date_success = success_updated.select("registration_date", "day", "month", "quarter", "year").distinct()\
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

# ##### Creation of the partner dimension table from success_updated

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
