# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('Medicare').getOrCreate()
spark

# COMMAND ----------

df1 = spark.read.parquet('/FileStore/tables/us_cities_dimension__1_-1.parquet')
display(df1.limit(3))
df1.count()

# COMMAND ----------

# Drop extra col
df_city = df1.select(
                    upper(df1.city).alias('city'),
                    df1.state_id,
                    upper(df1.state_name).alias('state_name'),
                    upper(df1.county_name).alias('county_name'),
                    df1.population,
                    df1.timezone,
                    df1.zips
                    )
display(df_city.limit(3))

df_city.printSchema()

# COMMAND ----------

df2 = spark.read.csv('/FileStore/tables/USA_Presc_Medicare_Data_12021.csv' , inferSchema=True , header=True)
display(df2.limit(3))
df2.printSchema()

df2.count()

# COMMAND ----------

# drop extra col 
df_presc = df2.select(
            df2.npi.alias('presc_id'),
            df2.nppes_provider_last_org_name.alias('presc_lname'),
            df2.nppes_provider_first_name.alias('presc_fname'),
            df2.nppes_provider_city.alias('presc_city'),
            df2.nppes_provider_state.alias('presc_state'),
            df2.specialty_description.alias('presc_speclty'),
             df2.total_claim_count.alias('tx_cnt'),
            df2.drug_name,
            df2.total_drug_cost,
            df2.total_day_supply,
            df2.years_of_exp           
            )
display(df_presc.limit(3))


# COMMAND ----------

# Concat fname , lname to name & Add new col 'country_name' 
from pyspark.sql.types import *

df_presc  = df_presc.withColumn('name' , concat_ws(' ','presc_fname','presc_lname'))
# 
df_presc = df_presc.withColumn('country_name' , lit('USA'))

df_presc = df_presc.drop('presc_fname' , 'presc_lname')

display(df_presc.limit(3))

# COMMAND ----------


#  Handling Missing values
def check_null(df):
    nulls = []
    for i in df.columns:
        null_count = count(when(isnan(i) | col(i).isNull(),i)).alias(i)
        nulls.append(null_count)
    df_null = df.select(nulls)
    display(df_null)

check_null(df_presc)

# COMMAND ----------

# Remove NULL Values
df_presc = df_presc.dropna(subset=['presc_id', 'drug_name'])
display(df_presc.limit(3))

# COMMAND ----------

# Checking if NULL values exist 
check_null(df_presc)

# COMMAND ----------

# Now, converting year_of_exp col from STR to INT

df_presc = df_presc.withColumn('years_of_exp', regexp_replace(col('years_of_exp'), r"^=" , "" ))

df_presc = df_presc.withColumn('years_of_exp' , col('years_of_exp').cast(DoubleType()))

display(df_presc.limit(3))


# COMMAND ----------

# Calculate mean for missing value of col 'tx_cnt'

mean_tx = df_presc.select(mean(col('tx_cnt'))).collect()[0][0]
print(mean_tx)

df_presc = df_presc.fillna(mean_tx , 'tx_cnt')

# COMMAND ----------

check_null(df_presc)


# COMMAND ----------

#  Create udf(User Define Function) 

from pyspark.sql.functions import *
from pyspark.sql.types import *

@udf(returnType = IntegerType())

def record_len(column):
    splitcol = column.split(' ')
    return len(splitcol)

# COMMAND ----------

df_city_zip = df_city.withColumn('zip_count' , record_len('zips'))
display(df_city_zip.select('zip_count').limit(6))

# COMMAND ----------

# Joining two dataset 
df_join = df_presc.join(
    df_city_zip , 
    (df_presc.presc_city == df_city_zip.city)&(df_presc.presc_state == df_city_zip.state_id) ,
    how='inner'
    )
display(df_join.limit(6))       

# COMMAND ----------


