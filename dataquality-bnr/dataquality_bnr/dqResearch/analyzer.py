from pyspark.sql import functions as F
from pyspark.sql.types import *

from datetime import datetime
import pandas as pd

import pydeequ
from pydeequ.analyzers import *

import yaml
from yaml.loader import SafeLoader


    
def getYamlData(yaml_path):
    yamlData_list = []

    with open(yaml_path, 'r') as f:
        yamlData = list(yaml.load_all(f, Loader=SafeLoader))[0]

    return yamlData

def addAnalyzers(analysisRunner, yamlData):

    for analyzerMethod_sentence in yamlData:
        print(analyzerMethod_sentence)
        analyzerMethod = eval(analyzerMethod_sentence)

        analysisRunner = analysisRunner.addAnalyzer(analyzerMethod)
        
    return analysisRunner

def getDateString():
    datetimeNow = datetime.now()
    
    day = str(datetimeNow.day).zfill(2)
    month = str(datetimeNow.month).zfill(2)
    year = str(datetimeNow.year)

    #International Standard yyyy-mm-dd
    date_string=year+"-"+month+"-"+day
    
    return date_string

def loadDataframe(spark, analysisResult):
    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
    
    analysisResult_df = analysisResult_df.drop("tagtable","entity")

    analysisResult_df = (analysisResult_df
                  .withColumnRenamed("instance","column")
                  .withColumnRenamed("name","metric")
                 )
    
    date_string = getDateString()
    analysisResult_df = analysisResult_df.withColumn("research_date",F.lit(date_string)) 
    
    return analysisResult_df

def runAnalyzer(spark, df, yaml_path):
    yamlData = getYamlData(yaml_path)
    
    analysisRunner = AnalysisRunner(spark).onData(df)
    analysisRunner = addAnalyzers(analysisRunner, yamlData)
    
    analysisResult = analysisRunner.run()
    
    analysisResult_df = loadDataframe(spark, analysisResult)
    
    return analysisResult_df



    