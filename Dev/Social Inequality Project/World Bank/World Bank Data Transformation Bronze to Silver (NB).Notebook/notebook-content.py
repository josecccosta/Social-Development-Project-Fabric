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
# META         },
# META         {
# META           "id": "e003063b-04a8-42a0-8e85-b0243d356adb"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_social = spark.read.table("bronze_lakehouse.world_bank.Social_Barriers")

# Limpeza de Valores Impossíveis e Nulos
# - Literacy e Internet não podem ser > 100 ou < 0
# - Child Mortality não pode ser negativa
df_cleaned = df_social.filter(
    (F.col("Country_Code").isNotNull()) & 
    (F.col("Year").isNotNull())
).withColumn(
    "Literacy_Rate", F.when(F.col("Literacy_Rate") > 100, 100).otherwise(F.col("Literacy_Rate"))
).withColumn(
    "Internet_Access", F.when(F.col("Internet_Access") > 100, 100).otherwise(F.col("Internet_Access"))
)

# Tratamento de Nulos (Forward Fill - Opcional mas Recomendado)
# Como o Banco Mundial não reporta todos os anos, preenchemos o ano vazio com o valor do ano anterior
window_spec = Window.partitionBy("Country_Code").orderBy("Year").rowsBetween(Window.unboundedPreceding, 0)

df_final = df_cleaned.withColumn(
    "Literacy_Rate", F.last("Literacy_Rate", ignorenulls=True).over(window_spec)
).withColumn(
    "School_Attendance", F.last("School_Attendance", ignorenulls=True).over(window_spec)
).withColumn(
    "Female_Account_Ownership", F.last("Female_Account_Ownership", ignorenulls=True).over(window_spec)
)

# Remover linhas onde todos os indicadores sociais estão vazios
# (Não nos serve ter um país/ano se não sabemos nada sobre ele)
indicadores = ["School_Attendance", "Literacy_Rate", "Internet_Access", 
               "Female_Account_Ownership", "Child_Mortality_Rate", "Life_Expectancy"]

df_final = df_final.na.drop(subset=indicadores, how='all')

# 5. Guardar a tabela limpa
df_final.write.format("delta").mode("overwrite").saveAsTable("bronze_lakehouse.world_bank.Social_Barriers")

print("✅ Limpeza concluída! Tabela 'Social_Barriers' pronta.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Carregar as duas tabelas
# Nota: Ajusta os nomes se os nomes das tabelas no catálogo forem diferentes
df_social = spark.read.table("bronze_lakehouse.world_bank.Social_Barriers")
df_mpi = spark.read.table("silver_lakehouse.dbo.MPI")

# 2. Realizar o Join
# Usamos "left" para manter todos os dados da Social_Barriers, 
# mesmo que não haja um MPI correspondente para aquele país/ano.
df_social_silver = df_social.join(
    df_mpi, 
    (df_social.Country_Code == df_mpi.Country_Code_Iso3) & (df_social.Year == df_mpi.Year), 
    "left"
)

# 3. Limpeza pós-join
# Como o join cria colunas duplicadas (Country_Code e Year), vamos selecionar apenas as que interessam
# e remover a coluna redundante do MPI
df_social_silver = df_social_silver.select(
    df_social["*"],          # Mantém todas as colunas da tabela social (incluindo Country_Code e Year)
    df_mpi["MPI"]            # Adiciona apenas a coluna do valor do MPI
)

# 4. Guardar na Camada Silver
table_name_silver = "silver_lakehouse.dbo.Social_Barriers"

df_social_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name_silver)

print(f"✅ Sucesso! A tabela '{table_name_silver}' foi criada com o MPI integrado.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Carregar as tabelas originais da Silver
df_fact = spark.read.table("silver_lakehouse.dbo.Social_Barriers")
df_geo = spark.read.table("silver_lakehouse.dbo.geography")

# --- PASSO A: ISOLAR AGREGADOS (WLD, SSA, HIC, etc.) ---
# Usamos 'left_anti' para ficar apenas com o que NÃO existe na geografia
df_aggregates = df_fact.join(
    df_geo, 
    df_fact.Country_Code_Iso3 == df_geo.Country_Code_Iso3, 
    "left_anti"
)

# Gravar a tabela de Benchmarks/Agregados
df_aggregates.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_lakehouse.dbo.Global_Social_Barriers")

# --- PASSO B: ISOLAR PAÍSES REAIS ---
# Usamos 'inner' join para garantir que só passam códigos que existem na Dim_Geography
df_fact_countries = df_fact.join(
    df_geo.select("Country_Code_Iso3"), 
    on="Country_Code_Iso3", 
    how="inner"
)

# Gravar a tabela de Factos principal (a que vai para o mapa)
df_fact_countries.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_lakehouse.dbo.Countries_Social_Barriers")

# --- PASSO C: DIAGNÓSTICO FINAL ---
print("🚀 Processo de Separação Concluído com Sucesso!")
print(f"📊 Registos Totais Originais: {df_fact.count()}")
print(f"🌍 Registos na Countries (Países): {df_fact_countries.count()}")
print(f"📈 Registos na Aggregates (Regiões/Mundo): {df_aggregates.count()}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Carregar, filtrar e sobrescrever a tabela Silver
df_silver_clean = spark.read.table("silver_lakehouse.dbo.Global_Social_Barriers") \
    .filter(F.col("Country_Code_Iso3") != "XKX")

df_silver_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_lakehouse.dbo.Global_Social_Barriers")

print("🗑️ Kosovo (XKX) removido da tabela Silver com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Ler a tabela da Bronze
df_econ_raw = spark.read.table("Bronze_LakeHouse.world_bank.Economic_Indicators")

# 2. Seleção estratégica e limpeza
df_econ_silver = df_econ_raw.select(
    F.col("Country").alias("Country_Code_Iso3"),
    F.col("Year").cast("int"),
    F.col("GDP_Per_Capita").cast("double"),
    F.col("Inflation_CPI_Pct").cast("double"),
    F.col("GDP_Growth_Annual_Pct").alias("GDP_Annual_Growth_Pct") # Renomear para clareza
).filter(F.col("Year") >= 2010) # Manter o teu filtro de tempo

# 3. Gravar na Silver
df_econ_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Silver_LakeHouse.dbo.Economic_Indicators")

print("Tabela Macro-Económica criada com PIB, Inflação e Crescimento!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Criar o DataFrame base
anos = spark.createDataFrame([(y,) for y in range(2010, 2025)], ["Year"])

# 2. Aplicar as transformações e forçar o tipo Integer no Year
dim_date = anos.withColumn("Year", F.col("Year").cast("int")) \
    .withColumn(
        "Decade", 
        F.concat((F.floor(F.col("Year") / 10) * 10).cast("string"), F.lit("s"))
    ).withColumn(
        "Economic_Context",
        F.when(F.col("Year") <= 2012, "Post-2008 Financial Crisis Recovery")
         .when((F.col("Year") >= 2013) & (F.col("Year") <= 2019), "Global Growth Period")
         .when((F.col("Year") >= 2020) & (F.col("Year") <= 2022), "COVID-19 Impact")
         .otherwise("Post-Pandemic Recovery")
    ).withColumn(
        "Global_Goals",
        F.when(F.col("Year") < 2015, "Millennium Development Goals (MDGs)")
         .otherwise("Sustainable Development Goals (SDGs)")
    )

# 3. Gravar na Silver
dim_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_lakehouse.dbo.Dim_Date")

print("✅ Dim_Date atualizada! O campo 'Year' agora é Integer.")
dim_date.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Carregar a Geografia atual e remover a coluna 'sub_region_name' extra
# (O erro mostrou que tens 'sub-region_name' e 'sub_region_name', vamos manter apenas a correta)
df_geo_current = spark.read.table("silver_lakehouse.dbo.Geography").drop("sub_region_name")

# 2. Criar o DataFrame de Taiwan SEM a coluna extra
schema_geo = StructType([
    StructField("region_name", StringType(), True),
    StructField("sub-region_name", StringType(), True),
    StructField("intermediate_region_name", StringType(), True),
    StructField("country_or_area", StringType(), True),
    StructField("Country_Code_Numeric", IntegerType(), True),
    StructField("Country_Code_Iso2", StringType(), True),
    StructField("Country_Code_Iso3", StringType(), True)
])

twn_data = [("Asia", "Eastern Asia", "Eastern Asia", "Taiwan", 158, "TW", "TWN")]
df_twn = spark.createDataFrame(twn_data, schema=schema_geo)

# 3. Unir e Gravar (Overwrite) para limpar o modelo da Silver
df_geo_final = df_geo_current.union(df_twn).distinct()

df_geo_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_lakehouse.dbo.Geography")

print("✅ Coluna extra removida e Taiwan adicionado à Silver.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_bronze = spark.read.table("world_bank.unemployment")


df_silver = df_bronze.select(
    F.col("country_code_iso3"),
    F.col("Year"),
    F.round("Unemployment_Total", 2).alias("Unemployment_Total"),
    F.round("Unemployment_Female", 2).alias("Unemployment_Female"),
    F.round("Unemployment_Male", 2).alias("Unemployment_Male")
).dropna(how='all', subset=['Unemployment_Total', 'Unemployment_Female', 'Unemployment_Male'])

df_silver = df_silver.dropDuplicates(['country_code_iso3', 'Year'])

catalog_name = "silver_lakehouse"
dbo_schema = "dbo"
table_name = "unemployment_rate"
full_path = f"{catalog_name}.{dbo_schema}.{table_name}"

print(f"🚀 A gravar dados arredondados em {full_path}...")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{dbo_schema}")

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(full_path)

print("✅ Processo concluído com arredondamento!")
df_silver.select("country_code_iso3", "Year", "Unemployment_Total").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
