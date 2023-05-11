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