<img width="259" height="53" alt="image" src="https://github.com/user-attachments/assets/333fc494-b334-4cec-8a33-58dd63a5f6ab" /># The Data Abyss: A Multidimensional Analysis of Social Inequality

# 1. Project Objectives
In the modern global economy, the gap between the "haves" and the "have-nots" is no longer just a matter of bank balances. It is a vast, multidimensional abyss that separates nations and regions across every facet of human existence. By synthesizing these diverse indicators, we can identify not just where inequality exists, but how it evolves across different economic contexts and global policy eras (MDGs vs. SDGs).

## 1.1 The Regional Divide
While global GDP has seen significant growth in recent decades, this prosperity has not been distributed equally. Inequality manifests differently depending on the geographic lens:

- **The Global North vs. Global South:** We see a stark contrast in "Social Barriers," where high-income regions enjoy near-universal literacy and internet access, while other regions struggle with basic infrastructure.
- **Intra-Regional Disparity:** Even within fast-growing regions, the "Gini Index" reveals that internal wealth concentration can be extreme, creating "islands of wealth" surrounded by systemic poverty.

## 1.2 Measuring the Abyss
To truly understand the depth of this inequality, this project moves beyond simple GDP metrics. We employ a multidimensional framework to compare countries using four critical pillars:

1. **Economic Concentration (The Wealth Gap):** Using the Income Share of the Top 1% and 10% to measure how much national wealth is captured by a tiny fraction of the population.

2. **Human Development (The Quality of Life):** Utilizing the Human Development Index (HDI) and Life Expectancy to measure the tangible outcomes of a country's social policies.

3. **Labor Market Stability:** Analyzing Unemployment Rates (Age 15+) and Monthly Earnings to see who is being left behind in the workforce, specifically looking at gender-based disparities.

4. **Institutional Barriers:** Tracking Literacy Rates and Social Barriers to identify the structural hurdles that prevent upward mobility.

## 2. Analytical Framework: Key Research Questions

&nbsp;

### 2.1. 📊 Diagnostic: Mapping the Abyss (Where & How Much?)

- **Global Distribution:** How have inequality (Gini Index) and development (HDI) varied across regions since 2000?
- **Wealth Extremes:** Which are the top 10 countries where the "Top 1%" earn the most, and what is their average wage?
- **The Inclusion Gap:** In countries with a rising Human Development Index (HDI), is wealth effectively reaching the bottom 50% of the population?

### 2.2. 🔍 Causality & Structural Barriers (The "Why")

- **Human Capital vs. Inequality:** Is there a measurable correlation between Literacy Rates (from the Social_Barriers table) and a reduction in the Gini Index over the last decade?
- **The Digital Divide:** Does increased internet access effectively reduce the unemployment rate?
- **Gender and Economy:** Do countries with higher levels of female financial inclusion have higher average wages?

### 2.3. 🚀 Predictive & Resilience Analysis (What Now?)

- **The Impact of Crises:** Did the 2008 financial crisis lead to a more permanent concentration of wealth than the COVID-19 pandemic?
- **Health as a Shield:** Did countries with better child health outcomes and higher longevity prove more resilient against unemployment shocks during the pandemic?
- **Barriers to Advancement:** Which factor most hinders wage growth: illiteracy, extreme poverty (Multidimensional Poverty Index - MPI), or poor health outcomes?

## 3. The Medallion Architecture in Microsoft Fabric
This project utilizes a Medallion Architecture to manage the data lifecycle, with each layer hosted in its own dedicated Lakehouse. This ensures a clear separation between raw ingestion and refined analytical data.

### 3.1. Bronze Layer: Raw Ingestion
The Bronze Lakehouse serves as the landing zone for all source data. The primary objective here was to move data from external providers into our environment while maintaining the original state.

- **File-Based Ingestion (CSV to Delta):** Data from the UN Census, ILOSTAT, UNDP Human Development Reports, and Our World in Data was ingested via CSV files. These were then converted into Delta tables to enable ACID transactions and improved performance.
- **API-Driven Ingestion (Python to Delta):** For the World Bank Open Data, I developed Python notebooks to call their API directly. The resulting Spark dataframes were saved as Delta tables.
- **Reference Data:** A master list of Standard Country and Area Codes was ingested as a CSV and converted to Delta, providing the backbone for our geographic hierarchy.

### 3.2. Silver Layer: Data Refinement and Integration
 In the Silver Lakehouse, we transition from raw data to a high-quality, reporting-ready format. This layer was developed using two distinct transformation paths based on the complexity of the source data.

#### 3.2.1. Standardized Processing (Dataflow Gen2)
For the UNdata, ILOSTAT, UNDP, and OWID datasets, the transformation pipeline was built using Dataflow Gen2. This allowed for a visual, scalable approach to cleaning:

- **Geographic Integrity:** A critical filtering step was performed to remove countries and territories that no longer exist. This ensures the model only reflects current geopolitical entities, preventing historical data conflicts.
- **Column Pruning:** Removed administrative and metadata columns that were not relevant to socio-economic analysis.
- **Table Consolidation:** Merged fragmented tables, such as the various income share percentiles (Top 1%, Top 10%, etc.), into unified fact tables.
- **Gender Normalization:** Extracted unique gender values to create a centralized Sex dimension table and calculated "Both Sexes" aggregate rows where only segmented data existed.
- **Quality Gates:** Implemented steps to filter out duplicate records and enforce schema consistency across all Delta tables.

#### 3.2.2. Advanced Transformations: World Bank Open Data
Because the World Bank data required more nuanced time-series handling and business logic, it was processed through specialized Python/Spark notebooks:

- **Iterative Cleaning:** Removed rows where primary metrics were null to ensure the accuracy of the Gini and Income datasets.
- **Forward-Fill Imputation:** To handle intermittent data reporting, I applied a forward-fill logic, ensuring that the most recent available data point is carried forward until a new record is found.
- **Feature Engineering (Economic Context):** I created a new categorical column, Economic_Context, to provide historical signaling for analysis. This allows users to filter data based on global economic cycles:
    - **Post-2008:**
        - **Global Growth Period:** Years of sustained economic expansion.
        - **COVID-19:** The specific period of global pandemic impact (2020–2022).
        - **Post-COVID Recovery:** Data from 2023 to 2024.

## 4. Data Dictionary & Table Summaries

&nbsp;

<div align="center">

| Table Name | Key Fields | Description |
| :--- | :--- | :--- |
| **Geography** | Country Name, Year, Region Name, Sub Region Name, Year | The central lookup table containing regional hierarchies and country identifiers. |
| **Gender** | Gender | A reference table used to slice labor and earnings data by gender. |
| **Countries_Social_Barriers** | Country Name, Year, Female Account Ownership, Internet Access, Literacy Rate, School Attendance, Life Expectancy and MPI | Contains key human capital metrics, including mortality and education rates. |
| **Income_Share** | Country Name, Year, Share Top 1%, Share Top 10%, Share Middle 40% and Share Bottom 50%  | Tracks wealth distribution across different population percentiles. |
| **Monthly_Employee_Earnings** | Country Name, Year, Monthly Earnings, Gender Code | Granular data on labor compensation, linked by country and gender. |
| **Unemployment_Rate** | Country Name, Year, Unemployement Rate, Gender Code | Monitors labor market stability across different demographic segments. |
| **HDI** | Country Name, Year, Human Development Index  | Global indices for human development |
| **Gini_Index** | Country Name, Year, Gini Index | Global indices for income inequality |
| **Economic_Indicators** | Country Name, Year, Gini Index | Global indices for income inequality | 


**Table 1:** *Data Dictionary for Labor and Economic Indicators*

</div>

&nbsp;

### 4.1. The Core Reference Tables
These tables provide the context (who and where) for all the metrics in the model.

- **Geography:** The "anchor" of the model. It contains standardized country names and codes (ISO2, ISO3, and Numeric) along with regional hierarchies (Region, Sub-region, and Intermediate region). This ensures that data from different sources can be mapped to the same physical location.

- **Gender:** A small but vital lookup table that defines gender categories. It allows the model to split labor and social metrics by gender for comparative analysis.

### 4.2. Economic & Inequality Tables
These tables track how wealth is distributed and how much people are earning.

- **Gini_Index:** Contains the Gini coefficient values per country and year, representing the degree of income inequality within a population.
- **Income_Share:** Provides a more granular look at wealth distribution by breaking down the percentage of national income held by specific groups, such as the bottom 50%, middle 40%, and the top 1% and 10%.
- **Monthly_Employee_Earnings:** Records the average or median wages earned by employees, categorized by sex and geographic location.

### 4.3. Social & Development Tables
These tables measure the "human" side of the data, focusing on quality of life and education.

- **HDI (Human Development Index)**: Stores the composite index scores that rank countries by life expectancy, education, and per capita income indicators.

- **Social_Barriers:** A multifaceted table covering diverse quality-of-life metrics, including child mortality, internet access, literacy rates, and school attendance. It also includes the Multi-dimensional Poverty Index (MPI).

### 4.4. Labor Market Tables

- **Unemployment_Rate:** Tracks the percentage of the labor force that is without work. This table is unique as it allows for slicing by both Age groups and Sex, providing a deep look into labor market health.

    - **Technical Specification:** This table utilizes the Working-Age Population standard, defined as individuals aged 15 years and older. By using the $15+$ threshold, the model aligns with **international labor standards**, ensuring that metrics remain comparable across diverse economies regardless of varying national retirement ages or local educational mandates.

### 4.5. Temporal Framework: The 2015 Pivot (MDGs vs. SDGs)
To provide meaningful analysis, the data in these tables is viewed through the lens of two major United Nations global frameworks. The year 2015 serves as the critical "watershed" year for the metrics collected in this model:

- **The MDG Era (2000 – 2015):** The Millennium Development Goals focused primarily on developing nations, targeting extreme hunger and child mortality. In tables like Social_Barriers, data from 2010–2014 represents the "final push" to meet these targets, often showing the sharpest declines in mortality rates in regions like Africa.

- **The SDG Era (2015 – 2030):** The Sustainable Development Goals expanded to 17 goals applicable to all nations (developed and developing alike).

- **Impact on the Model:** This transition explains the shift in data granularity. Indicators such as Gender Equality (captured in our Sex and Earnings tables) and Digital Access (internet connectivity in Social_Barriers) gained central importance in global statistics only after this 2015 pivot.

## 5. Semantic Models

### 5.1 Silver Semantic Model
<img width="532" height="567" alt="Screenshot 2026-01-30 161455" src="https://github.com/user-attachments/assets/3a49b2f8-3d51-42f8-be37-5fe9e57eaabf" />

## 6. Challenges 

### 6.1. Challenges in Data Acquisition (UN Census)
The most significant barrier encountered was the 100,000-record limit on CSV exports. For a report requiring longitudinal or multi-regional analysis, 100,000 rows is an insufficient threshold. This constraint necessitated a fragmented approach to data collection:

- **Manual Batching:** The dataset had to be manually partitioned into smaller subsets based on specific variables (e.g., individual years or specific sub-regions) to ensure each export stayed under the limit.
- **Workflow Inefficiency:** This "slicing" of data converted what should have been a single download into a repetitive, multi-stage process, significantly increasing the time spent on data ingestion rather than analysis.


### 6.2. Incomplete Datasets. 
The integrity of the analysis was frequently challenged by incomplete datasets, specifically regarding temporal and geographic coverage.

**Key issues encountered:**

- **Lagging Indicators (The "2024 Gap"):** While the report timeframe extends to 2024, several core indicators—most notably Social Barriers (Life Expectancy)—had not yet been updated by the source providers for the final year. This forced the analysis for 2024 to rely on a subset of available economic metrics rather than a full socio-economic picture.
- **Temporal Fragmentation:** Aside from the 2024 gap, several tables lacked records for specific historical years, creating "blind spots" that made it difficult to establish consistent long-term trends.
- **Geographic Bias:** Certain regions lacked the required statistical density. This missingness often results in a skewed comparison where only data-rich regions are represented, potentially biasing the final global insights.

### 6.3. Geopolitical Data Decay
A specific challenge in data cleaning involved the presence of obsolete geopolitical entities.

- **Historical Overlap:** Raw datasets often included records for countries or territories that no longer exist or have undergone significant administrative changes.
- **Resolution:** To ensure the Silver layer remained relevant for modern reporting, I had to manually identify and remove these "ghost" countries. This was necessary to prevent the inflation of regional averages and to ensure that the Geography dimension table remained a reflection of the current world map.

### 6.4. Redundant and Static Values.
A significant observation during the data profiling stage was the presence of repetitive data points across various nations and consecutive years.

**Technical Implications:**

- **Imputed Stagnation:** The recurrence of identical values suggests that "Carry Forward" or "Mean Imputation" methods were used by the source provider. This masks real-world volatility and can lead to "flat" visualizations that don't reflect actual trends.
- **Lack of Granularity:** Identical values across different countries often indicate that regional averages were applied to individual nations where specific data was missing, reducing the precision of country-level comparisons.

### 6.5. Infrastructure and Resource Constraints (Microsoft Fabric)
In addition to data-specific hurdles, the technical environment posed significant operational limitations. The project was executed within a shared workspace utilizing a low-tier Microsoft Fabric capacity, which introduced several bottlenecks:

- **Resource Contention:** Operating within a shared workspace meant that available compute power was divided among multiple concurrent users and processes. This often led to "throttling," where execution times for notebooks and pipelines increased during peak usage periods.
- **Concurrency and Queueing:** The most persistent issue was the lack of parallel processing overhead. Due to the low capacity limits, the workspace struggled to handle multiple active sessions. This resulted in frequent job queuing or outright failures when one team member attempted to run a notebook while another was refreshing a semantic model.

