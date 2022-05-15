from pyspark.sql import functions as F
from pyspark.sql.types import *

from datetime import datetime
import pandas as pd

import pydeequ
from pydeequ.profiles import *
from pydeequ.repository import *



def hadoopDeletePath(spark, path):
    #path:  /tmp/x266727/sampleRepository/metricsFile/apaga.json
    
    sc = spark.sparkContext
    hadoop = sc._jvm.org.apache.hadoop
    conf = sc._jsc.hadoopConfiguration()

    fs = hadoop.fs.FileSystem.get(conf)

    if fs.exists(hadoop.fs.Path(path)):
        fs.delete(hadoop.fs.Path(path), True)
        return 0
    else:
        print("Error: .hadoopDeletePath FAILED TO DELETE HADOOP PATH")
        return 1


def getResultKey(spark): 
    key_tags = {'tagTable': 'myTable'}
    
    resultKey = ResultKey(spark, ResultKey.current_milli_time(), key_tags)
    
    return resultKey
    
def getRepository(spark, metrics_file):
    #metrics_file: "/tmp/module_research/profile/repository_1.json"
    
    repository = FileSystemMetricsRepository(spark, metrics_file)
    
    return repository

def getDateString():
    datetimeNow = datetime.now()
    
    day = str(datetimeNow.day).zfill(2)
    month = str(datetimeNow.month).zfill(2)
    year = str(datetimeNow.year)

    #International Standard yyyy-mm-dd
    date_string=year+"-"+month+"-"+day
    
    return date_string
    
def loadDataframe(repository, resultKey):
    dataSetDate = resultKey.resultKey.dataSetDate()
    
    profile_df = (repository
                  .load()
                  .after(dataSetDate - 1)
                  .getSuccessMetricsAsDataFrame())
    
    profile_df = profile_df.drop("tagtable","entity","dataset_date")

    profile_df = (profile_df
                  .withColumnRenamed("instance","column")
                  .withColumnRenamed("name","metric")
                 )
    
    date_string = getDateString()
    profile_df = profile_df.withColumn("research_date",F.lit(date_string)) 
    
    return profile_df

def runProfile(spark, df, metrics_file):
    repository = getRepository(spark, metrics_file)
    resultKey = getResultKey(spark)
    
    result = (ColumnProfilerRunner(spark)
        .onData(df)
        .useRepository(repository)
        .saveOrAppendResult(resultKey)
        .run())

    #load profile_df from repository
    profile_df = loadDataframe(repository, resultKey)
    
    #delete tmp repository
    hadoopDeletePath(spark, metrics_file)
    
    return profile_df
    
    
    
    