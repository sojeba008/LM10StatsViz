# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 00:00:18 2023

@author: Jean-Baptiste
"""

import os
import re
from dotenv import load_dotenv
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_replace, monotonically_increasing_id, lit
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import DataFrameWriter

findspark.init()
findspark.add_packages(["org.postgresql:postgresql:42.6.0"])

dotenv_path = os.path.join(os.getcwd(), '.env')
load_dotenv(dotenv_path)

def result(score):
    if  int(score.split(":")[0])>int(score.split(":")[1]):
        return "Win"
    elif int(score.split(":")[0])==int(score.split(":")[1]):
        return "Draw" 
    else :
        return "Defeat"

def removeSpacialChar(s):
    s = re.sub(r'[^\x00-\x7F]+', '', s)
    return s


def convertMinuteToInt(minute):
    if "+" not in minute:
        return minute
    else:
        return int(minute.split("+")[0])+int(minute.split("+")[1])
    

class LM10ClubGoalStatsETL:
    def __init__(self):
      
        self.postgres_url = os.getenv("postgres_url")
        self.properties = {
            "user": os.getenv("user"),
            "password": os.getenv("password"),
            "driver": os.getenv("driver")
        }
        
        spark = SparkSession.builder.getOrCreate()
        self.data = spark.read.csv("input/data.csv", header=True, encoding="UTF-8")
        self.data.printSchema()
        self.data = self.data.withColumn("pk_fact", monotonically_increasing_id() + 1)
        self.data = self.data.fillna(" ")
        #self.data = self.data.withColumn("Goal_assist", regexp_replace(self.data["Goal_assist"], "(\\t|null)$", ""))
        self.data = self.data.withColumn("Date", regexp_replace(self.data["Date"],"-","/" ))
        self.data = self.data.withColumn("Playing_Position", regexp_replace(self.data["Playing_Position"], " ", ""))
        removeSpacialChar_udf = udf(removeSpacialChar, StringType())
        #self.data = self.data.withColumn("Opponent", removeSpacialChar_udf(self.data["Opponent"])) 
        self.data = self.data.withColumn("Goal_assist", removeSpacialChar_udf(self.data["Goal_assist"])) 
        
    
    def persitDataInDB(self, table_name, data):
        data_writer = DataFrameWriter(data)
        data_writer.jdbc(url=self.postgres_url, table=table_name, mode="overwrite", properties=self.properties)

    def loadDimCompetiton(self):
        self.allCompetitions = self.data.select("Competition").withColumnRenamed("Competition", "name").distinct().withColumn("pk_competition", monotonically_increasing_id() + 1)
        self.allCompetitions.show()
        table_name = "public.dim_competition"
        self.persitDataInDB(table_name, self.allCompetitions)
    
    def loadDimDate(self):
        self.data = self.data.withColumn('month', split(self.data["date"], "/").getItem(0))
        self.data = self.data.withColumn('day', split(self.data["date"], "/").getItem(1))
        self.data = self.data.withColumn('year', split(self.data["date"], "/").getItem(2))
        self.data = self.data.withColumn("pk_goal", monotonically_increasing_id() + 1)
        self.allDates = self.data.select("day", "month", "year").distinct().withColumn("pk_date", monotonically_increasing_id() + 1)
        self.allDates.show()
        table_name = "public.dim_date"
        self.persitDataInDB(table_name, self.allDates)
        
    def loadDimType(self): 
        self.allTypes = self.data.select("Type").withColumnRenamed("Type", "name").distinct().withColumn("pk_type", monotonically_increasing_id() + 1)
        self.allTypes.show()
        table_name = "public.dim_type"
        self.persitDataInDB(table_name, self.allTypes)
        #self.data = self.data.join(self.allTypes, [self.allTypes.name == self.data.Type], "inner").drop("name")


    def loadDimGoalAssist(self):
        self.allGoalAssist = self.data.select("Goal_assist").withColumnRenamed("Goal_assist", "name").distinct().withColumn("pk_goal_assist", monotonically_increasing_id() + 1)
        #self.allGoalAssist = self.allGoalAssist.withColumn("name", regexp_replace(self.allGoalAssist["name"], "(\\t|null)$", ""))
        self.allGoalAssist.show()
        table_name = "public.dim_goal_assist"
        self.persitDataInDB(table_name, self.allGoalAssist)        

    def loadDimResult(self):
        result_udf = udf(result, StringType())
        self.data = self.data.withColumn("result", result_udf(self.data["At_score"]))
        self.allResult = self.data.select("result").distinct().withColumn("pk_result", monotonically_increasing_id() + 1)
        table_name = "public.dim_result"
        self.persitDataInDB(table_name, self.allResult)

    def loadDimClub(self):
        self.allMessiClubs = self.data.select("Club").distinct().withColumnRenamed("Club", "name")
        self.allMessiOponents = self.data.select("Opponent").distinct().withColumnRenamed("Opponent", "name")
        self.allClub = self.allMessiClubs.unionByName(self.allMessiOponents).distinct()
        self.allClub = self.allClub.withColumn("pk_club", monotonically_increasing_id() + 1)
        self.allClub.show()
        table_name = "public.dim_club"
        self.persitDataInDB(table_name, self.allClub)

    def loadDimVenue(self):
        self.allVenue = self.data.select("Venue").distinct()
        self.allVenue = self.allVenue.withColumn("pk_venue", monotonically_increasing_id() + 1)
        self.allVenue.show()
        table_name = "public.dim_Venue"
        self.persitDataInDB(table_name, self.allVenue)

    def loadDimPlayingPosition(self):
        self.allMessiPlayingPosition = self.data.select("Playing_Position").distinct().withColumn("pk_playing_position", monotonically_increasing_id() + 1)
        self.allMessiPlayingPosition = self.allMessiPlayingPosition.withColumnRenamed("Playing_Position", "code")
        self.allMessiPlayingPosition.show()
        table_name = "public.dim_playing_position"
        self.persitDataInDB(table_name, self.allMessiPlayingPosition)
        
    def loadMinuteGoal(self): 
        self.allMinuteGoal = self.data.select("Minute").distinct().withColumnRenamed("Minute", "minute_str")
        convertMinuteToInt_udf = udf(convertMinuteToInt, StringType())
        self.allMinuteGoal = self.allMinuteGoal.withColumn("minute_int", convertMinuteToInt_udf(self.allMinuteGoal["minute_str"])).withColumn("pk_minute", monotonically_increasing_id() + 1)
        self.allMinuteGoal.show()
        table_name = "public.dim_minute_goal"
        self.persitDataInDB(table_name, self.allMinuteGoal)

    def loadFactMessiGoal(self):     
        self.data = self.data.join(self.allCompetitions, self.allCompetitions.name == self.data.Competition, "inner").drop("name")
        self.allCompetitions.unpersist()
        self.data = self.data.join(self.allDates, [self.allDates.month == self.data.month, self.allDates.day == self.data.day, self.allDates.year == self.data.year], "inner").drop("name")
        self.allDates.unpersist()
        self.data = self.data.join(self.allTypes, [self.allTypes.name == self.data.Type], "inner").drop("name")
        self.allTypes.unpersist()
        self.data = self.data.join(self.allGoalAssist, [self.allGoalAssist.name == self.data.Goal_assist], "inner").drop("name")
        self.allGoalAssist.unpersist()
        self.data = self.data.join(self.allResult, [self.allResult.result == self.data.result], "inner").drop("result")
        self.allResult.unpersist()
        self.data = self.data.join(self.allVenue, [self.allVenue.Venue == self.data.Venue], "inner").drop("Venue")
        self.allVenue.unpersist()
        self.data = self.data.join(self.allMessiPlayingPosition, [self.allMessiPlayingPosition.code == self.data.Playing_Position], "inner")
        self.allMessiPlayingPosition.unpersist()
        self.data = self.data.join(self.allMinuteGoal, [self.allMinuteGoal.minute_str == self.data.Minute], "inner")
        self.data = self.data.join(self.allClub, [self.allClub.name == self.data.Opponent], "inner").drop("name")
        self.allClub.unpersist()
        table_name = "public.fact_goal"
        factMessiGoal = self.data.select("pk_date", "pk_competition", "pk_type", "pk_goal_assist", "pk_result", "pk_club", "pk_venue", "pk_playing_position", "pk_minute").withColumn("goal",  lit(1))
        self.persitDataInDB(table_name, factMessiGoal)
        factMessiGoal.unpersist()
        self.data.unpersist()
        
        
etl = LM10ClubGoalStatsETL()
etl.loadDimCompetiton()
etl.loadDimDate()
etl.loadDimType()
etl.loadDimGoalAssist()
etl.loadDimResult()
etl.loadDimClub()
etl.loadDimVenue()
etl.loadDimPlayingPosition()
etl.loadMinuteGoal()
etl.loadFactMessiGoal()
