# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 00:00:18 2023

@author: Jean-Baptiste
"""
def persitDataInDB(table_name, data):
    postgres_url = "jdbc:postgresql://192.168.8.146:5432/messiStatsDW"
    properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    data_writer = DataFrameWriter(data)
    data_writer.jdbc(url=postgres_url, table=table_name, mode="overwrite", properties=properties)



import findspark
findspark.init()
findspark.add_packages(["org.postgresql:postgresql:42.6.0"])
from pyspark.sql import SparkSession

from pyspark.sql.functions import split, regexp_replace


from pyspark.sql.functions import monotonically_increasing_id, lit

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import DataFrameWriter


spark = SparkSession.builder.getOrCreate()
data = spark.read.csv("c:/Users/sojeb/Downloads/data.csv", header=True, encoding="UTF-8")
data = data.withColumn("Goal_assist", regexp_replace(data["Goal_assist"], "(\\t|null)$", ""))
data = data.withColumn("Date", regexp_replace(data["Date"],"-","/" ))
data.printSchema()

allCompetitions = data.select("Competition").withColumnRenamed("Competition", "name").distinct().withColumn("pk_competition", monotonically_increasing_id() + 1)
allCompetitions.show()

##
data = data.join(allCompetitions, allCompetitions.name == data.Competition, "inner").drop("name")
##

### Dim date
data = data.withColumn('month', split(data["date"], "/").getItem(0))
data = data.withColumn('day', split(data["date"], "/").getItem(1))
data = data.withColumn('year', split(data["date"], "/").getItem(2))
data = data.withColumn("pk_goal", monotonically_increasing_id() + 1)
allDates = data.select("day", "month", "year").distinct().withColumn("pk_date", monotonically_increasing_id() + 1)
allDates.show()

postgres_url = "jdbc:postgresql://192.168.8.146:5432/messiStatsDW"
properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
table_name = "public.dim_date"
persitDataInDB(table_name, allDates)

##
data = data.join(allDates, [allDates.month == data.month, allDates.day == data.day, allDates.year == data.year], "inner").drop("name")
##
####################


####Dim goal type
allTypes = data.select("Type").withColumnRenamed("Type", "name").distinct().withColumn("pk_type", monotonically_increasing_id() + 1)
allTypes.show()
table_name = "public.dim_type"
persitDataInDB(table_name, allTypes)

##
data = data.join(allTypes, [allTypes.name == data.Type], "inner").drop("name")
##
#####################

###Dim assist
allGoalAssist = data.select("Goal_assist").withColumnRenamed("Goal_assist", "name").distinct().withColumn("pk_goal_assist", monotonically_increasing_id() + 1)
#allGoalAssist = allGoalAssist.withColumn("name", regexp_replace(allGoalAssist["name"], "(\\t|null)$", ""))
allGoalAssist.show()
table_name = "public.dim_goal_assist"
persitDataInDB(table_name, allGoalAssist)
##
data = data.join(allGoalAssist, [allGoalAssist.name == data.Goal_assist], "inner").drop("name")
##
################

###Dim result
def result(score):
    if  int(score.split(":")[0])>int(score.split(":")[1]):
        return "Win"
    elif int(score.split(":")[0])==int(score.split(":")[1]):
        return "Draw" 
    else :
        return "Defeat"
result_udf = udf(result, StringType())
data = data.withColumn("result", result_udf(data["At_score"]))
allResult = data.select("result").distinct().withColumn("pk_result", monotonically_increasing_id() + 1)
table_name = "public.dim_result"
persitDataInDB(table_name, allResult)

##
data = data.join(allResult, [allResult.result == data.result], "inner").drop("result")
##
##############

####dim club
allMessiClubs = data.select("Club").distinct().withColumnRenamed("Club", "name")
allMessiOponents = data.select("Opponent").distinct().withColumnRenamed("Opponent", "name")
allClub = allMessiClubs.unionByName(allMessiOponents)
allClub = allClub.withColumn("pk_club", monotonically_increasing_id() + 1)
allClub.show()
table_name = "public.dim_club"
persitDataInDB(table_name, allResult)

##
data = data.join(allClub, [allClub.name == data.Opponent], "inner").drop("name")
##
##############
####Dim venue
allVenue = data.select("Venue").distinct()
allVenue = allVenue.withColumn("pk_venue", monotonically_increasing_id() + 1)
allVenue.show()
table_name = "public.dim_Venue"
persitDataInDB(table_name, allVenue)

##
data = data.join(allVenue, [allVenue.Venue == data.Venue], "inner").drop("Venue")
##
####################


####dim playing postion
allMessiPlayingPosition = data.select("Playing_Position").distinct().withColumn("pk_playing_position", monotonically_increasing_id() + 1)
allMessiPlayingPosition = allMessiPlayingPosition.withColumn("Playing_Position", regexp_replace(data["Playing_Position"], " ", "")).withColumnRenamed("Playing_Position", "code")
allMessiPlayingPosition.show()
table_name = "public.dim_playing_position"
persitDataInDB(table_name, allMessiPlayingPosition)
##
data = data.join(allMessiPlayingPosition, [allMessiPlayingPosition.code == data.Playing_Position], "inner")
##
##################


###Dim minute goal
AllMinuteGoal = data.select("Minute").distinct().withColumnRenamed("Minute", "minute_str")
def convertMinuteToInt(minute):
    if "+" not in minute:
        return minute
    else:
        return int(minute.split("+")[0])+int(minute.split("+")[1])
convertMinuteToInt_udf = udf(convertMinuteToInt, StringType())
AllMinuteGoal = AllMinuteGoal.withColumn("minute_int", convertMinuteToInt_udf(AllMinuteGoal["minute_str"])).withColumn("pk_minute", monotonically_increasing_id() + 1)
AllMinuteGoal.show()
table_name = "dim_minute_goal"
persitDataInDB(table_name, allMessiPlayingPosition)
##
data = data.join(AllMinuteGoal, [AllMinuteGoal.minute_str == data.Minute], "inner")
##



table_name = "fact_goal_messi"
messiGoalFact = data.select("pk_date", "pk_competition", "pk_type", "pk_goal_assist", "pk_result", "pk_club", "pk_venue", "pk_playing_position", "pk_minute").withColumn("goal",  lit(1))
persitDataInDB(table_name, messiGoalFact)