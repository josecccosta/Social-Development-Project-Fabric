# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2040f8e7-720b-4901-acd5-9b9c700b12af",
# META       "default_lakehouse_name": "Bronze_LakeHouse",
# META       "default_lakehouse_workspace_id": "d83c184e-82f0-4705-952c-0e29c5cb5274",
# META       "known_lakehouses": [
# META         {
# META           "id": "2040f8e7-720b-4901-acd5-9b9c700b12af"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "10c4d00b-2c47-88c7-4f76-b00347d57f02",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # (1) Create Delta Tables for the World Bank data for the Bronze Layer

# MARKDOWN ********************

# # (1.1) Educational Data

# MARKDOWN ********************

# ### (1.1.1) High-Level Educational Attainment across the global population aged 25 and older. Specifically, it tracks the "highest level of schooling completed" for three distinct tiers of tertiary (higher) education.

# CELL ********************

"""
import wbgapi as wb
import pandas as pd

# 1. Target all countries instead of a specific list
countries = 'all'

# 2. Educational indicators (Mapping World Bank codes to readable names)
edu_indicators = {
    'SE.TER.CUAT.MS.FE.ZS': 'Master_Female_Pct',
    'SE.TER.CUAT.MS.MA.ZS': 'Master_Male_Pct',
    'SE.TER.CUAT.ST.MA.ZS': 'Short_Cycle_Male_Pct',
    'SE.TER.CUAT.DO.FE.ZS': 'Doctoral_Female_Pct',
    'SE.TER.CUAT.BA.FE.ZS': 'Bachelor_Female_Pct',
    'SE.TER.CUAT.ST.FE.ZS': 'Short_Cycle_Female_Pct',
    'SE.TER.CUAT.DO.MA.ZS': 'Doctoral_Male_Pct',
    'SE.TER.CUAT.BA.MA.ZS': 'Bachelor_Male_Pct'
}

# 3. Fetch the data
# We'll keep mrv=40 because education surveys are often infrequent/sporadic
df_edu = wb.data.DataFrame(
    list(edu_indicators.keys()), 
    countries, 
    mrv=40, 
    columns='series'
)

df_edu = wb.data.DataFrame(list(edu_indicators.keys()), 'cnt', time=range(1980, 2025), columns='series')

# 4. Cleaning and Formatting
df_edu = df_edu.rename(columns=edu_indicators)

df_edu = (
    df_edu
    .reset_index()
    .rename(columns={'economy': 'Country', 'time': 'Year'})
    .sort_values(['Country', 'Year'])
    .reset_index(drop=True)
)

# Set display to 2 decimal places for percentages
pd.options.display.float_format = '{:,.2f}%'.format

print("Educational Attainment Indicators (Population 25+)")

schema = "world_bank"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# If using PySpark to save the Delta Table
df_spark = spark.createDataFrame(df_edu)

df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("description", "Global educational attainment metrics for population 25+ sourced from World Bank API") \
    .saveAsTable(f"{schema}.educational_attainment_pct")
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.2) Development Data
# The indicators you've listed fall into two primary categories: Socio-economic Development and Infrastructure Performance.
# Specifically, these are used by organizations like the World Bank and the UN to measure a country's energy profile. Here is the breakdown of how they are classified:
# 
# ### 1. Energy Access Indicators
# The three "Access to electricity" metrics are social development indicators. They measure the reach of the power grid and the equity of service across different demographics.
# - Access to electricity (% of population): A macro-level indicator of national development.
# - Urban vs. Rural Access: These are distributional indicators. They highlight geographic inequality and are crucial for identifying where infrastructure investment is most needed (often referred to as "the last mile" problem).
# 
# ### 2. Energy Consumption Indicators
# Electric power consumption (kWh per capita): This is an economic intensity indicator. It measures the average amount of electrical energy used per person.
# - High kWh per capita usually correlates with high industrialization and higher standards of living.
# - Low kWh per capita often indicates a lack of industrial base or energy poverty.


# CELL ********************

import wbgapi as wb
import pandas as pd

# 1. Target all countries
countries = 'all'

# 2. Electricity indicators mapping
# EG.USE.ELEC.KH.PC: Electric power consumption (kWh per capita)
# EG.ELC.ACCS.ZS: Access to electricity (% of population)
# EG.ELC.ACCS.UR.ZS: Access to electricity, urban (% of urban population)
# EG.ELC.ACCS.RU.ZS: Access to electricity, rural (% of rural population)
elec_indicators = {
    'EG.USE.ELEC.KH.PC': 'KWh_Per_Capita',
    'EG.ELC.ACCS.ZS': 'Access_Total_Pct',
    'EG.ELC.ACCS.UR.ZS': 'Access_Urban_Pct',
    'EG.ELC.ACCS.RU.ZS': 'Access_Rural_Pct'
}

# 3. Fetch the data
# mrv=20 is usually sufficient for electricity trends
df_elec = wb.data.DataFrame(
    list(elec_indicators.keys()), 
    countries, 
    mrv=40, 
    columns='series'
)

# 4. Cleaning and Formatting
df_elec = df_elec.rename(columns=elec_indicators).reset_index()

# Extract Year as integer (removes 'YR' prefix)
df_elec['time'] = df_elec['time'].str.replace('YR', '').astype(int)

df_elec = (
    df_elec
    .rename(columns={'economy': 'Country_Code', 'time': 'Year'})
    .sort_values(['Country_Code', 'Year'])
    .reset_index(drop=True)
)

# 5. Save to Spark Delta Table
schema = "world_bank"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

table_name = "electricity_access_consumption"

# Create Spark DataFrame
df_spark = spark.createDataFrame(df_elec)

# Write to Delta
df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("description", "Global electricity access and consumption metrics sourced from World Bank API") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"Data successfully saved to {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.3) Demographic Data
# ### 1. Component Breakdown
# #### The Population Pillars (Static Counts)
# - **Total Population:** The baseline count of all residents. This serves as the "denominator" for nearly every social and economic KPI (Key Performance Indicator).
# - **Gender Distribution (Male/Female):** Essential for identifying demographic imbalances. In data analysis, this is used to calculate gender-specific literacy, employment, and health outcomes.
# 
# #### The Migration Dynamics (Movement & Stock)
# International Migrant Stock: Represents the total number of people living in a country who were born elsewhere. This is a measure of a country's diversity and attractiveness as a destination.
# - **Net Migration:** The "Pulse" of a country's borders. It calculates (In-migration - Out-migration).
# - **Positive Net Migration:** Indicates a "Pull" factor (economic opportunity, safety).
# - **Negative Net Migration:** Indicates a "Push" factor (economic hardship, brain drain).


# CELL ********************

import wbgapi as wb
import pandas as pd

# 1. Target all countries
countries = 'all'

# 2. Demographic indicators mapping
# SP.POP.TOTL: Population, total
# SP.POP.TOTL.MA.IN: Population, male
# SP.POP.TOTL.FE.IN: Population, female
# SM.POP.TOTL: International migrant stock, total
# SM.MET.NETM: Net migration
demo_indicators = {
    'SP.POP.TOTL': 'Pop_Total_Count',
    'SP.POP.TOTL.MA.IN': 'Pop_Male_Count',
    'SP.POP.TOTL.FE.IN': 'Pop_Female_Count',
    'SM.POP.TOTL': 'Migrant_Stock_Total_Count',
    'SM.MET.NETM': 'Net_Migration_Flow'
}

# 3. Fetch the data
# mrv=40 to capture historical migration trends (often reported in 5-year gaps)
df_demo = wb.data.DataFrame(
    list(demo_indicators.keys()), 
    countries, 
    mrv=100, 
    columns='series'
)

# 4. Cleaning and Formatting
df_demo = df_demo.rename(columns=demo_indicators).reset_index()

# Clean Year column (World Bank API returns 'YR2020', we want 2020)
df_demo['time'] = df_demo['time'].str.replace('YR', '').astype(int)

df_demo = (
    df_demo
    .rename(columns={'economy': 'Country_Code', 'time': 'Year'})
    .sort_values(['Country_Code', 'Year'])
    .reset_index(drop=True)
)

# 5. Save to Spark Delta Table
schema = "world_bank"
table_name = "population_migration"

# Create Spark DataFrame
df_spark = spark.createDataFrame(df_demo)

# Write to Delta
df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("description", "Global population counts and migration flows sourced from World Bank API") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"Demographic data successfully saved to {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### (1.3.1) Fertility

# CELL ********************

import wbgapi as wb
import pandas as pd

# 1. Target all countries
countries = 'all'

# 2. Fertility indicator mapping
# SP.DYN.TFRT.IN: Fertility rate, total (births per woman)
fertility_indicators = {
    'SP.DYN.TFRT.IN': 'Fertility_Rate_Births_Per_Woman'
}

# 3. Fetch the data
# mrv=40 is great for seeing long-term demographic transitions
df_fertility = wb.data.DataFrame(
    list(fertility_indicators.keys()), 
    countries, 
    mrv=100, 
    columns='series'
)

# 4. Cleaning and Formatting
df_fertility = df_fertility.rename(columns=fertility_indicators).reset_index()

# Extract Year as integer
df_fertility['time'] = df_fertility['time'].str.replace('YR', '').astype(int)

df_fertility = (
    df_fertility
    .rename(columns={'economy': 'Country_Code', 'time': 'Year'})
    .sort_values(['Country_Code', 'Year'])
    .reset_index(drop=True)
)

# 5. Save to Spark Delta Table
schema = "world_bank"
table_name = "fertility_rates"

# Create Spark DataFrame
df_spark = spark.createDataFrame(df_fertility)

# Write to Delta
df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("description", "Total fertility rate (births per woman) sourced from World Bank API") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"Fertility data successfully saved to {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## (1.4) Economic Data

# MARKDOWN ********************

# ### (1.4.1) Economic Indicators

# CELL ********************

import pandas as pd
import wbgapi as wb

# 1. Configuração: Nomes legíveis e sem caracteres especiais (essencial para Delta Lake)
econ_map = {
    'NY.GDP.MKTP.CD': 'GDP_USD',
    'NY.GDP.PCAP.CD': 'GDP_Per_Capita',
    'NY.GDP.MKTP.KD.ZG': 'GDP_Growth_Annual_Pct', # Removido o %
    'FP.CPI.TOTL.ZG': 'Inflation_CPI_Pct',         # Removido o %
    'NY.GDP.MKTP.PP.CD': 'GDP_PPP'
}

countries = 'all'

# 2. Recolha de Dados da API
print("A descarregar dados da API (isto pode demorar 1-2 min)...")
df_econ = wb.data.DataFrame(list(econ_map.keys()), countries, mrv=40, columns='series')

# 3. Limpeza de Dados (Pandas)
df_econ = (
    df_econ
    .rename(columns=econ_map)
    .reset_index()
    .rename(columns={'economy': 'Country', 'time': 'Year'})
)

# CORREÇÃO: Transforma 'YR2020' em 2020 (inteiro). O Spark prefere números a strings.
df_econ['Year'] = df_econ['Year'].str.replace('YR', '').astype(int)

df_econ = df_econ.sort_values(['Country', 'Year']).reset_index(drop=True)

# 4. Configurações de Destino
schema = "world_bank"
table_name = "economic_indicators"

# 5. Garantir que o Esquema existe no Lakehouse antes de gravar
print(f"A criar/verificar esquema '{schema}'...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# 6. Converter para Spark e Gravar no Delta Lake
print(f"A gravar a tabela {schema}.{table_name} no Lakehouse...")
df_spark = spark.createDataFrame(df_econ)

df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("description", "Economic indicators sourced from World Bank API") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"Sucesso! Dados guardados em {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### (1.4.2) Employment Indicators

# CELL ********************

import wbgapi as wb
import pandas as pd

# 1. Configuração
countries = 'all'
employment_indicators = {
    'SL.UEM.TOTL.NE.ZS': 'Unemployment_Total_Pct',
    'SL.UEM.TOTL.MA.NE.ZS': 'Unemployment_Male_Pct',
    'SL.UEM.TOTL.FE.NE.ZS': 'Unemployment_Female_Pct',
    'SL.UEM.1524.NE.ZS': 'Youth_Unemployment_Total_Pct',
    'SL.UEM.1524.MA.NE.ZS': 'Youth_Unemployment_Male_Pct',
    'SL.UEM.1524.FE.NE.ZS': 'Youth_Unemployment_Female_Pct'
}

# 2. Fetch Data (Voltando aos 40 anos)
print("1. A descarregar 40 anos de dados da API... (Isto pode levar 2-3 min)")
df_employment = wb.data.DataFrame(list(employment_indicators.keys()), countries, mrv=40, columns='series')

# 3. Limpeza e Formatação
df_employment = df_employment.rename(columns=employment_indicators).reset_index()
df_employment['time'] = df_employment['time'].str.replace('YR', '').astype(int)
df_employment = df_employment.rename(columns={'economy': 'Country_Code', 'time': 'Year'})

# --- TRATAMENTO PARA O SPARK NÃO TRAVAR ---
# Converte colunas numéricas para float explicitamente e preenche vazios com None (que o Spark entende como NULL)
cols_to_fix = list(employment_indicators.values())
for col in cols_to_fix:
    df_employment[col] = pd.to_numeric(df_employment[col], errors='coerce')

df_employment = df_employment.sort_values(['Country_Code', 'Year']).reset_index(drop=True)

# 4. Gravação no Delta Table
schema = "world_bank"
table_name = "unemployment_rates_global"

print(f"2. A gravar em {schema}.{table_name}...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

df_spark = spark.createDataFrame(df_employment)

df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"✓ SUCESSO! Dados de 40 anos guardados em {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Tabelas para as perguntas nivel 2

# CELL ********************

import wbgapi as wb
import pandas as pd

# 1. Configuração dos indicadores
# Ajustei os nomes para serem compatíveis com SQL/Delta (sem espaços ou símbolos)
nivel_2_WB = {
    'SE.XPD.TOTL.GD.ZS' : 'Gov_Education_Exp_Pct_GDP',
    'SE.SEC.NENR' : 'School_Enrollment_Secondary_Net_Pct',
    'IT.NET.USER.ZS' : 'Internet_Usage_Pct_Pop',
    'SI.POV.DDAY' : 'Poverty_Headcount_Ratio_2_15_Day',
    'FX.OWN.TOTL.FE.ZS' : 'Account_Ownership_Female_Pct',
    'WP_time_01.2' : 'Digital_Payments_Past_Year_Female_Pct',
    'SL.TLF.CACT.FE.ZS' : 'Labor_Force_Participation_Female_Pct'
}

countries = 'all'

# 2. Recolha de Dados
print("A descarregar indicadores de desenvolvimento (mrv=40)...")
df_social = wb.data.DataFrame(list(nivel_2_WB.keys()), countries, mrv=40, columns='series')

# 3. Limpeza e Formatação
df_social = df_social.rename(columns=nivel_2_WB).reset_index()

# Transformar o ano em Inteiro
df_social['time'] = df_social['time'].str.replace('YR', '').astype(int)

df_social = (
    df_social
    .rename(columns={'economy': 'Country_Code', 'time': 'Year'})
    .sort_values(['Country_Code', 'Year'])
    .reset_index(drop=True)
)

# 4. Configuração de Destino no Fabric
schema = "world_bank"
table_name = "social_development_indicators"

# Garantir que o esquema existe
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# 5. Criar Spark DataFrame e Gravar
print(f"A gravar a tabela {schema}.{table_name} no Lakehouse...")
df_spark = spark.createDataFrame(df_social)

df_spark.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("description", "Social and education indicators (Gini, Poverty, Education) sourced from World Bank") \
    .saveAsTable(f"{schema}.{table_name}")

print(f"Dados sociais guardados com sucesso em {schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
