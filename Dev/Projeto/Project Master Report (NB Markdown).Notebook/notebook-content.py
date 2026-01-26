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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Data Engineering Project: UN and  World Bank Data Analysis

# MARKDOWN ********************

# ## 1. Project Objectives
# The goal of this project is to build an end-to-end data pipeline that correlates national demographic trends with global economic indicators to provide a holistic view of a country's development trajectory. This project analyzes the socio-economic landscape by merging granular population data with high-level financial and social metrics.


# MARKDOWN ********************

# ## 2. Data Sources
# + **UN Census Data** (for demographic information): 
#   + This dataset provides the "human" foundation of the project. It contains detailed demographic information collected directly from national authorities, including age distribution, sex ratios, literacy rates, and urban vs. rural living 
#   conditions. It tells us who the people are and where they live (often provided in 10-year intervals).
#   + https://unstats.un.org/unsd/demographic-social/products/dyb/dybcensusdata.cshtml
# + **World Bank Global Development data**:
#   + This dataset provides the "contextual" layer. Known as World Development Indicators (WDI), it includes annual metrics on GDP growth, life expectancy, poverty headcount, and labor force participation. It tells us how a country is performing economically and socially on the global stage (often provided in 1-year intervals).
#   + https://data.worldbank.org/

# MARKDOWN ********************

# ## (3) Data Summary
# ### (3.1) Demographic Statistics
# Sourced primarily from the UN Demographic Yearbook, this section tracks the fundamental structure and movement of human populations.
# 
# - **Population Composition:** Snapshot of the human landscape categorized by age, sex, and urban/rural residence.
# - **Employment Status:** Categorizes the economically active population into Employees, Employers, Own-Account Workers, and Contributing Family Workers, cross-referenced by age and sex.
# - **Unemployment Rate:** Measures labor force underutilization, focusing on the Gender Unemployment Gap and Youth Unemployment (15–24) over historical periods.
# - **Migration & Totals:** Tracks absolute population counts (male/female) alongside Net Migration Flow to identify "Brain Gain" or "Brain Drain" scenarios.
# - **Fertility Rate:** Uses the 2.1 "Replacement Level" benchmark to correlate population growth with industrialization and infrastructure needs.
# 
# ### (3.2) Education Statistics
# This data assesses the global "educational stock" and real-time enrollment trends using the ISCED framework.
# 
# - **Educational Attainment:** Classifies the population (15+) by highest completed level, ranging from No Schooling to Tertiary (ISCED 5-8).
# - **School Attendance:** Tracks real-time enrollment for ages 5–24, identifying dropout points and the Gender Parity Index in urban vs. rural settings.
# - **High-Level Achievement:** Specifically monitors the percentage of the population (25+) with Doctoral, Master’s, Bachelor’s, or Short-Cycle Tertiary degrees, disaggregated by sex.
# 
# ### (3.3) Development Statistics
# Focuses on infrastructure as a proxy for economic maturity and energy poverty.
# 
# - **Electricity Access:** Monitors the Urban-Rural Divide in grid connectivity.
# - **Energy Intensity:** Measures kWh per capita to correlate power consumption with industrialization and household income.
# 
# ### (3.4) Economic Statistics
# Evaluates the financial health, individual prosperity, and labor market efficiency of nations.
# 
# - **Economic Performance:**  GDP (USD vs. PPP): Compares absolute market value against purchasing power to adjust for cost-of-living differences.
# - **GDP Per Capita:** The primary indicator of individual well-being and its correlation with education.
# - **Growth & Inflation:** Tracks annual expansion and the Consumer Price Index (CPI) to measure economic stability.
# - **Labor Under utilization:** Analyzes national unemployment estimates, specifically the Gender Unemployment Gap, while acknowledging "hidden" factors like informal work and discouraged workers.
# 
# ### (3.5) Project Data Framework


# MARKDOWN ********************

# 
# | Category | Dataset | Primary Metrics | Key Dimensions |
# | :--- | :--- | :--- | :--- |
# | **Demographics** | Population Structure | Total Count, Sex Ratio | Age, Sex, Urban/Rural |
# | | Employment Status | Status (Employee, Employer, etc.) | Risk/Authority, Age, Sex |
# | | Unemployment | Unemployment Rate (%) | Sex, Youth (15–24) vs. Adult (15+) |
# | | Migration & Fertility | Net Migration, Total Fertility Rate (TFR) | Inflow/Outflow, 2.1 Replacement Level |
# | **Education** | Attainment (15+) | Highest Level Completed (ISCED) | No Schooling through Tertiary (Level 8) |
# | | Attendance (5–24) | Enrollment/Dropout Rates | Single-year Age, Urban/Rural, Sex |
# | | Tertiary Specialist | % Doctoral, Master’s, Bachelor's | Sex-disaggregated Achievement |
# | **Development** | Infrastructure | Electricity Access (%) | Urban vs. Rural (Energy Poverty) |
# | | Consumption | kWh per capita | Economic Intensity, Grid Efficiency |
# | **Economics** | Performance | GDP (USD), GDP (PPP), Growth % | Absolute Power vs. Cost of Living |
# | | Prosperity | GDP per capita, Inflation (CPI) | Standard of Living, Price Stability |
# | | Labor Utilization | National Unemployment Estimates | Gender Gap, Informal Work Narrative |
# 
# <p align="center"><i>Table 1: Summary of Project Data Indicators</i></p>


# MARKDOWN ********************

# ## 4 Challenges 
# 
# ### 4.1 Challenges in Data Acquisition (UN Census)
# The most significant barrier encountered was the 100,000-record limit on CSV exports. For a report requiring longitudinal or multi-regional analysis, 100,000 rows is an insufficient threshold. This constraint necessitated a fragmented approach to data collection:
# 
# - **Manual Batching:** The dataset had to be manually partitioned into smaller subsets based on specific variables (e.g., individual years or specific sub-regions) to ensure each export stayed under the limit.
# 
# - **Workflow Inefficiency:** This "slicing" of data converted what should have been a single download into a repetitive, multi-stage process, significantly increasing the time spent on data ingestion rather than analysis.
