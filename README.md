Medicare Data Analysis with PySpark on Databricks
In continuation of my last project, I created this one as a level-up, using Databricks to leverage PySpark for scalable data processing and analysis in healthcare. This project builds on prior work, incorporating more advanced data integration and transformation techniques to analyze Medicare prescription data by region in the U.S. By connecting prescription data to geographic data on Databricks, this project aims to uncover trends and costs associated with prescriptions across cities, counties, and states.

Project Overview
This project uses PySpark on Databricks to perform several tasks in data processing and transformation, addressing key questions about Medicare prescription claims and costs across the U.S.:

Data Loading and Preprocessing
How can we load and preprocess U.S. city and Medicare prescription data using PySpark?

The project reads U.S. cities data from a Parquet file and Medicare prescription data from a CSV file, preparing them for further analysis.
Data Cleaning and Standardization
How can we clean and standardize city and prescription data for consistent analysis?

We select relevant columns, rename them for clarity, and standardize text fields (e.g., converting city and state names to uppercase) to ensure consistency.
Regional Analysis of Prescription Claims
How can we identify patterns in Medicare prescription claims across cities and states?

By joining the city and prescription data based on location fields, we enable region-based insights, breaking down prescription claims by city, state, and county.
Cost and Claim Analysis by Region
What are the total counts and costs of prescriptions across different regions?

The script processes fields such as total_claim_count and total_drug_cost, allowing for analysis of prescription volume and costs at various geographic levels.
Leveraging PySpark on Databricks for Scalable Data Analysis
How can we use Databricks and PySpark to efficiently handle and analyze large healthcare datasets?

This project demonstrates Databricks' robust capabilities, enabling high-performance data ingestion, transformation, and aggregation, ideal for healthcare data analysis.
Setup and Usage
Environment
This project is implemented on Databricks, using PySpark for efficient data processing.

Run the Analysis

Open the notebook on Databricks and execute the cells in sequence to load, preprocess, and analyze the data.
Additional transformations or aggregations can be added to fit specific analysis needs.
Files
us_cities_dimension: Parquet file with U.S. city details, including state, county, population, and timezone.
USA_Presc_Medicare_Data: CSV file with Medicare prescription data, detailing prescriber information, drug details, claim counts, and costs.
Requirements
Databricks environment
PySpark
