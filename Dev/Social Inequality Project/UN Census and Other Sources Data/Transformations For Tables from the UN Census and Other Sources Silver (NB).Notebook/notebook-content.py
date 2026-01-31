# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e003063b-04a8-42a0-8e85-b0243d356adb",
# META       "default_lakehouse_name": "Silver_LakeHouse",
# META       "default_lakehouse_workspace_id": "d83c184e-82f0-4705-952c-0e29c5cb5274",
# META       "known_lakehouses": [
# META         {
# META           "id": "e003063b-04a8-42a0-8e85-b0243d356adb"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # (1) SQL code used to create Dimension Tables for the UN Census and other Silver Tables

# MARKDOWN ********************

# # (1.1) Transformations for the geography

# CELL ********************

from pyspark.sql import functions as F

df_geo = spark.read.table("silver_lakehouse.dbo.geography")

df_geo_fixed = df_geo.withColumn(
    "region_name", 
    F.when((F.col("region_name").isNull()) | (F.col("region_name") == ""), 
           F.col("country_or_area")).otherwise(F.col("region_name"))
).withColumn(
    "sub-region_name", 
    F.when((F.col("`sub-region_name`").isNull()) | (F.col("`sub-region_name`") == ""), 
           F.col("region_name")).otherwise(F.col("`sub-region_name`"))
).withColumn(
    "intermediate_region_name",
    F.when((F.col("intermediate_region_name").isNull()) | (F.col("intermediate_region_name") == ""), 
           F.col("`sub-region_name`")).otherwise(F.col("intermediate_region_name"))
)

df_geo_fixed.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_lakehouse.dbo.geography")

print("✅ Dados gravados. A atualizar esquema para visualização...")


spark.catalog.refreshTable("silver_lakehouse.dbo.geography")
df_final = spark.read.table("silver_lakehouse.dbo.geography")

df_final.select("country_or_area", "region_name", "`sub-region_name`", "intermediate_region_name") \
    .filter(F.col("country_or_area").contains("Antarctica")) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
