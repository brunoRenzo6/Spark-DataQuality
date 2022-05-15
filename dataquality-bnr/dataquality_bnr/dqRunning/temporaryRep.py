from pyspark.sql import functions as F
from pyspark.sql.types import *

from pydeequ.repository import *

import json

from dataquality_bnr.yamlHandler import infrastructure as yI

####################################################################################
#             Get jsonDataframe outOf persistentRepository
####################################################################################

def getLevel4(df):
    columnModifier_instance = (F.when(F.col("instance")=="*", F.lit("")).otherwise(F.col("instance")))
    columnBuilder_analyzer = (F.struct(F.col("name").alias("analyzerName"),
                                       columnModifier_instance.alias("column")))
    columnBuilder_metric = (F.struct(F.col("entity"),
                                     F.col("instance"),
                                     F.lit("DoubleMetric").alias("metricName"),
                                     F.col("name"),
                                     F.col("value")))
    
    df_lvl4 = (df
               .withColumn("analyzer", columnBuilder_analyzer)
               .withColumn("metric", columnBuilder_metric))
    return df_lvl4

def getLevel3(df):
    columnBuilder_metricMap_struct = (F.struct(F.col("analyzer"), F.col("metric")))
    
    dropColumns = ("analyzer", "metric")
    
    df_lvl3 = (df
               .withColumn("metricMap_struct", columnBuilder_metricMap_struct)
               .drop(*dropColumns))
    return df_lvl3

def getLevel2(df):
    df_lvl2 = (df
               .groupBy("dataset_date")
               .agg(F.collect_list("metricMap_struct").alias("metricMap"))
               .withColumnRenamed("dataset_date", "dataSetDate")
               .withColumn("tags", F.struct(F.lit(None).cast(StringType()).alias("tagX"))))
    return df_lvl2

def getLevel1(df):
    dropColumns = ("metricMap", "dataSetDate", "tags")
    df_lvl1 = (df
               .withColumn("analyzerContext", F.struct(F.col("metricMap")))
               .withColumn("resultKey", F.struct(F.col("dataSetDate"), F.col("tags")))
               .drop(*dropColumns))
    return df_lvl1

def getJsonDataframe(persistentRepository):
    metricColumns=["entity","instance","name","value","dataset_date","YY_MM_DD","tags"]
    successMetricsRepository = persistentRepository.select(*[metricColumns])

    dropColumns = ("entity", "instance", "name", "value", "YY_MM_DD", "tags")

    jsonDataframe = (getLevel4(successMetricsRepository).drop(*dropColumns))
    jsonDataframe = (getLevel3(jsonDataframe))
    jsonDataframe = (getLevel2(jsonDataframe))
    jsonDataframe = (getLevel1(jsonDataframe))

    return jsonDataframe

####################################################################################
#             Get Repository outOf jsonDataframe
####################################################################################

def getJsonObject_list(jsonDataframe):
    jsonObject_list=[]
    for i_str in jsonDataframe.toJSON().collect():
        jsonObject = json.loads(i_str)
        jsonObject_list.append(jsonObject)
    return jsonObject_list

def hdfsWrite(spark, tmpHDFSPath, fileContent):
    sc = spark.sparkContext
    hadoop = sc._jvm.org.apache.hadoop
    conf = sc._jsc.hadoopConfiguration()
    filePath = hadoop.fs.Path(tmpHDFSPath)
    fs = filePath.getFileSystem(conf)

    # create datastream and write out file (overwrite)
    dataStream = fs.create(filePath)
    dataStream.write(fileContent.encode('utf-8'))
    dataStream.close()
    
def writeJsonStructureToHDFS(spark, tmpHDFSPath, jsonDataframe):
    fileContent = getJsonObject_list(jsonDataframe)
    fileContent = str(fileContent)
    hdfsWrite(spark, tmpHDFSPath, fileContent)
    
def loadRepositoryFromHDFS(spark, tmpHDFSPath):
    metrics_file = tmpHDFSPath
    r = FileSystemMetricsRepository(spark, metrics_file)
    return r

def getRepository(spark, tmpHDFSPath, jsonDataframe):
    writeJsonStructureToHDFS(spark, tmpHDFSPath, jsonDataframe)
    
    r = loadRepositoryFromHDFS(spark, tmpHDFSPath)

    return r


####################################################################################
#                           Gennerate temporaryRepository
####################################################################################

def buildTemporaryRepository(spark, filtered_df, view_path):
    print("buildTemporaryRepository...")

    hdfsPath_repository = view_path+"/temporaryRepository/repository.json"

    if filtered_df==None:
        repository = FileSystemMetricsRepository(spark, hdfsPath_repository)
    else:
        jsonDataframe = getJsonDataframe(filtered_df)
        repository = getRepository(spark, hdfsPath_repository, jsonDataframe)
    return repository