from pyspark.sql import functions as F
from pyspark.sql.types import *

from datetime import datetime
import pandas as pd

import yaml
from yaml.loader import SafeLoader

import pydeequ
from pydeequ.suggestions import *
from pydeequ.analyzers import *
from pydeequ.profiles import *

####################################################################
#                          DataframeGennerator
#################################################################### 

def getYamlData(yaml_path):
    yamlData_list = []

    with open(yaml_path, 'r') as f:
        yamlData_list = list(yaml.load_all(f, Loader=SafeLoader))[0]

    yamlData = yamlData_list[0]

    return yamlData

def getGenneratorPattern(dfGennerator_dict):
    if "hdfs_path" in dfGennerator_dict.keys():
        if "table_name" in dfGennerator_dict.keys():
            if "sql_query" in dfGennerator_dict.keys():
                return "pattern1"

    if "sql_query" not in dfGennerator_dict.keys():
        if "table_name" not in dfGennerator_dict.keys():
            if "hdfs_path" in dfGennerator_dict.keys():
                return "pattern2" 

    if "sql_query" in dfGennerator_dict.keys():
        return "pattern3"

def fixSQLQueryIdentation(string):
    return " ".join(string.replace("\n", " ").split())

def getDataframe(spark, yaml_path):

    dfGennerator_dict = getYamlData(yaml_path)
    gennerator_pattern = getGenneratorPattern(dfGennerator_dict)

    df_filtered=""
    if gennerator_pattern == "pattern1":
        hdfs_path = dfGennerator_dict["hdfs_path"]
        table_name = dfGennerator_dict["table_name"]
        sql_query = dfGennerator_dict["sql_query"]

        df = spark.read.parquet(hdfs_path)

        df.createOrReplaceTempView(table_name)

        sql_query = fixSQLQueryIdentation(sql_query)
        df_filtered = spark.sql(sql_query)
        
    elif gennerator_pattern == "pattern2":
        hdfs_path = dfGennerator_dict["hdfs_path"]

        df_filtered = spark.read.parquet(hdfs_path)

    elif gennerator_pattern == "pattern3":
        sql_query = dfGennerator_dict["sql_query"]

        sql_query = fixSQLQueryIdentation(sql_query)
        df_filtered = spark.sql(sql_query)
    else:
        raise ValueError("inputData.yaml structure not recognized")

    return df_filtered
    

        