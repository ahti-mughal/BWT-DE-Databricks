%md
# Notebook Commands
* UI Intro
* magic commands
 #command
msg = "hello sheroze"
print(msg)
 #command

%sql
SELECT "hello"
 #command

%fs
ls
 #command

%fs
ls dbfs:/databricks-datasets/
 #command

%fs
ls dbfs:/databricks-datasets/COVID/
ls dbfs:/databricks-datasets/COVID/USAFacts/
 #command

%fs
ls dbfs:/databricks-datasets/COVID/USAFacts/
 #command

%fs
head dbfs:/databricks-datasets/COVID/USAFacts/covid_confirmed_usafacts.csv
%sh
ps
 #command



#video3
 #command
%fs
ls dbfs:/user/hive
 #command

%fs
ls dbfs:/user/hive/warehouse

 #command

%fs
ls dbfs:/user/hive/warehouse/circuits



#vidoe4

 #command
for folder in dbutils.fs.ls('/'):
    print(folder)
dbutils.fs.help()
dbutils.fs.help('mount')
dbutils.notebook.run('./Notebook introduction',10)
dbutils.notebook.run("./child_notebook", 10)
dbutils.widgets.help()
dbutils.widgets.text("input","","send the parameter value")
input_param=dbutils.widgets.get("input")
print(input_param)

 #command
%pip install pandas
 #command

%fs
ls
dbutils.notebook.exit(100)



# week 2
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
