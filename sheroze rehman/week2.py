%md
### ingestion circuit data
%md
### 1) read data 
display(dbutils.fs.mounts())
%fs
ls /databricks-datasets/adult/adult.data

covid_df = spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/databricks-datasets/adult/adult.data")
display(covid_df)
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
covid_schema = StructType(fields=[StructField("State-gov",StringType(),True),
                                    StructField("Bachelors",StringType(),True),
                                    StructField("Never-married",StringType(),True),
                                    StructField("Adm-clerical",StringType(),True),
                                    StructField("Not-in-family",StringType(),True),
                                    StructField("White",StringType(),True)])
covid_df = spark.read.option("header",True).schema(covid_schema).csv("dbfs:/databricks-datasets/airlines/")
display(covid_df)
covid_df.printSchema()
covid_df.display()
covid_df.describe().show()
covid_select_df=covid_df.select(covid_df("State-gov"),covid_df("Never-married"))
covid_select_df=covid_df.select("State-gov","Never-married")
from pyspark.sql.functions import col
covid_select_df=covid_df.select("State-gov","Never-married".alias("not_married"))
display(covid_select_df)
covid_renamed_df=covid_df.withColumnRenamed("Never-married","never_married").withColumnRenamed("Adm-clerical","adm_clerical").withColumnRenamed("State-gov","state_gov").withColumnRenamed("Not-in-family","not_family")
%md
###adding columnn
from pyspark.sql.functions import current_timestamp , lit
covid_final_df=covid_renamed_df.withColumn("ingestion_date",current_timestamp()).withColumn("env",lit("Production"))
display(covid_final_df)
%md
####write to dataframe as parquet
covid_final_df.write.parquet("/mnt/formula1dl/processed/circuits")
%fs
ls /mnt/formula1dl/processed/circuits
df=spark.read.parquet("/mnt/formula1dl/processed/circuits")
