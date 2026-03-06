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
from pyspark.sql.functions import row_number, when, col, isnan, isnull, round, lower, create_map, lit
from itertools import chain
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
                                 .withColumn("partner_id", row_number().over(Window.orderBy("business_partner", "financial_institution")))

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

# CELL ********************

# Importing the dataset success_updated
activity_updated_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_silver.Lakehouse/Tables/dbo/activity_updated"

# Load the success_updated table into a DataFrame
activity_updated = spark.read.format("delta").load(activity_updated_path) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Creation of the date dimension table from activity_updated

# CELL ********************

date_activity = activity_updated.select("activity_date", "day_activity", "month_activity", "quarter_activity", "year_activity").distinct()\
                            .withColumnRenamed("activity_date", "activity_date_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Creation of the support_type dimension table from activity_updated

# CELL ********************

support_activity = activity_updated.select("support_type").distinct()\
                                 .withColumn("support_name", when(col("support_type")== "sup01", "Training")\
                                 .when(col("support_type")== "sup02", "Coaching")\
                                 .otherwise("Business legalization"))\
                                 .withColumnRenamed("support_type", "support_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Linking the partner_success table to the original dataset activity_updated via the existing partner_key into the dataframe.

# CELL ********************

# Mapping dictionaries
partner_mapping = {
    "ccf1": 5,
    "fp1": 13,
    "gec1": 14,
    "ced1": 6,
    "cf1": 8,
    "gc1": 15,
    "mc1": 17,
    "sik1": 20,
    "aid1": 2,
    "kn1": 16
}

department_mapping = {
    "Nord": 5,
    "Sud": 8,
    "Centre": 2
}

# Convert dictionaries to Spark map expressions
partner_map_expr = create_map([lit(x) for x in chain(*partner_mapping.items())])
department_map_expr = create_map([lit(x) for x in chain(*department_mapping.items())])

# Apply transformations
activity_updated = (
    activity_updated
    .withColumn("partner_id", partner_map_expr[col("partner_key")])
    .withColumn("area_id", department_map_expr[col("department")])
    .drop("partner_key", "department")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ###### Create the fact table from the original dataset success_silver

# CELL ********************

# Select all required variables to create the fact table activity_fact
activity_fact = activity_updated.select('activity_id', 'activity_date', 'support_type', 'partner_id', 'area_id', 'activity_theme', 'activity_duration', 'attendance_number')\
                             .withColumnRenamed('support_type', "support_id")\
                             .withColumnRenamed('activity_date', 'activity_date_id')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Save the support_activity table as delta format to be saved into the gold Lakehouse.

# CELL ********************

# Save the support_activity into the Silver Lakehouse as delta format
support_activity_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_silver.Lakehouse/Tables/dbo/support_activity"
support_activity.write.format("delta").mode("overwrite").save(support_activity_path)

# Save the support_activity into the gold lakehouse
gold_support_activity_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/support_activity"
support_activity.write.format("delta").mode("overwrite").save(gold_support_activity_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Save the fact and dimension tables extracted from the original dataset activity_updated into the Lakehouse Gold.

# CELL ********************

# Generate each of path related to every table.
gold_activity_fact_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/activity_fact"
gold_date_activity_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/date_activity"

# Save the tables into the gold Lakehouse
activity_fact.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_activity_fact_path)
date_activity.write.format("delta").mode("overwrite").save(gold_date_activity_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3. Processing case of the dataset sales_updated by extracting fact and dimension tables.
# * Importing the dataset from the Lakehouse Silver.
# * Extract all fact and dimension tables. 
# * Generate Key primary and foreign one.
# * Remove non-required variables.

# CELL ********************

# Importing the dataset success_updated
sales_updated_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_silver.Lakehouse/Tables/dbo/sales_updated"

# Load the success_updated table into a DataFrame
sales_updated = spark.read.format("delta").load(sales_updated_path) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Creation of the date dimension table from sales_updated

# CELL ********************

date_sales = sales_updated.select("sales_reporting_date", "day_sales_reporting", "month_sales_reporting", "quarter_sales_reporting", "year_sales_reporting").distinct()\
                            .withColumnRenamed("sales_reporting_date", "date_sales_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Create the fact table from the original dataset sales_updated

# CELL ********************

# Select all required variables to create the fact table sales_fact
sales_fact = sales_updated.select('sales_key', 'msme_key', 'sales_reporting_date', 'domestic_sale', 'international_sale')\
                             .withColumnRenamed('sales_reporting_date', "date_sales_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Save the fact and dimension tables extracted from the original dataset sales_updated into the Lakehouse Gold.

# CELL ********************

# Generate each of path related to every table.
gold_sales_fact_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/sales_fact"
gold_date_sales_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/date_sales"

# Save the tables into the gold Lakehouse
sales_fact.write.format("delta").mode("overwrite").save(gold_sales_fact_path)
date_sales.write.format("delta").mode("overwrite").save(gold_date_sales_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4. Processing case of the dataset job_updated by extracting fact and dimension tables.
# * Importing the dataset from the Lakehouse Silver.
# * Extract all fact and dimension tables. 
# * Generate Key primary and foreign one.
# * Remove non-required variables.

# CELL ********************

# Importing the dataset success_updated
job_updated_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_silver.Lakehouse/Tables/dbo/job_updated"

# Load the success_updated table into a DataFrame
job_updated = spark.read.format("delta").load(job_updated_path) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Creation of the date dimension table from job_updated

# CELL ********************

date_job = job_updated.select("collecting_date", "day", "month", "quarter", "year").distinct()\
                            .withColumnRenamed("collecting_date", "date_job_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Create the fact table from the original dataset job_updated

# CELL ********************

# Select all required variables to create the fact table job_fact
job_fact = job_updated.select('job_key', 'msme_key', 'collecting_date', 'total_hired_employee', 'total_fired_employee')\
                             .withColumnRenamed('collecting_date', "date_job_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Save the fact and dimension tables extracted from the original dataset job_updated into the Lakehouse Gold.

# CELL ********************

# Generate each of path related to every table.
gold_job_fact_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/job_fact"
gold_date_job_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/date_job"

# Save the tables into the gold Lakehouse
job_fact.write.format("delta").mode("overwrite").save(gold_job_fact_path)
date_job.write.format("delta").mode("overwrite").save(gold_date_job_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 5. Processing case of the dataset firing_updated by anonymizing it.
# * Importing the dataset from the Lakehouse Silver.
# * Remove non-required variables.

# CELL ********************

# Importing the dataset success_updated
firing_updated_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_silver.Lakehouse/Tables/dbo/firing_updated"

# Load the success_updated table into a DataFrame
firing_updated = spark.read.format("delta").load(firing_updated_path) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Removing all non necessary variables (confidential data) from the original dataset firing_updated

# CELL ********************

# Select all required variables to create the fact table job_fact
firing = firing_updated.select('lic_key', 'job_key', 'sex', 'age')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Save the firing table extracted from the original dataset firing_updated into the Lakehouse Gold.

# CELL ********************

# Generate each of path related to every table.
gold_firing_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/firing"

# Save the tables into the gold Lakehouse
firing.write.format("delta").mode("overwrite").save(gold_firing_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 6. Processing case of the dataset hiring_updated by anonymizing it.
# * Importing the dataset from the Lakehouse Silver.
# * Remove non-required variables.

# CELL ********************

# Importing the dataset success_updated
hiring_updated_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_silver.Lakehouse/Tables/dbo/hiring_updated"

# Load the success_updated table into a DataFrame
hiring_updated = spark.read.format("delta").load(hiring_updated_path) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Removing all non necessary variables (confidential data) from the original dataset hiring_updated

# CELL ********************

# Select all required variables to create the fact table job_fact
hiring = hiring_updated.select('employee_key', 'job_key', 'age', 'sex', 'contract_duration', 'is_full_time', 'have_insurance', 'thanks_to_project_support', 'is_decent_work')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Save the hiring table extracted from the original dataset hiring_updated into the Lakehouse Gold.

# CELL ********************

# Generate each of path related to every table.
gold_hiring_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/hiring"

# Save the tables into the gold Lakehouse
hiring.write.format("delta").mode("overwrite").save(gold_hiring_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 7. Load and save the dataset participant_updated into the Lakehouse gold

# CELL ********************

# Importing the dataset success_updated
participant_updated_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_silver.Lakehouse/Tables/dbo/participant_updated"

# Load the success_updated table into a DataFrame
participant_updated = spark.read.format("delta").load(participant_updated_path) 

# Generate the path to save the participant_updated into the Lakehouse gold.
gold_participant_updated_path = "abfss://success_pipeline@onelake.dfs.fabric.microsoft.com/LH_success_gold.Lakehouse/Tables/dbo/participant_updated"

# Save the tables into the gold Lakehouse
participant_updated.write.format("delta").mode("overwrite").save(gold_participant_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
