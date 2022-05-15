from pyspark.sql import functions as F
from pyspark.sql.types import *

from datetime import datetime

from dataquality_bnr.yamlHandler import verificationSuite as yVS

from pydeequ.repository import *
from pydeequ.verification import *


def addCheckConfigs(checkResult_df, checkParameters):
    df=checkResult_df

    checkLevel = checkParameters["level"]
    description = checkParameters["description"]
    
    df = df.withColumn("check", F.lit(description))
    df = df.withColumn("check_level",F.when(F.col("check_level")=="Success", F.col("check_level"))
                       .otherwise(F.lit(checkLevel)))
    df = df.withColumn("check_status",
                       F.when(F.col("check_status")=="Success", F.col("check_status"))
                       .otherwise(F.lit(checkLevel)))
    
    return df

def addDatasetDate(successMetrics_df, resultKeyParameters):
    df=successMetrics_df
    
    dataset_date = resultKeyParameters["dataset_date"]
    
    df=df.withColumn("dataset_date",F.lit(dataset_date))
    
    return df

def addKeyTags(successMetrics_df, resultKeyParameters):
    df=successMetrics_df
    
    key_tags = resultKeyParameters["key_tags"]
    
    tags=[]
    for k in key_tags:
        key = str(k)
        value = str(key_tags[k])
        tags.append(key+":"+value)
        
    df=df.withColumn("tags",F.array([F.lit(tag) for tag in tags]))
    
    return df

def addResultKey(successMetrics_df, resultKeyParameters):
    df=successMetrics_df
    
    df = addDatasetDate(df, resultKeyParameters)
    df = addKeyTags(df, resultKeyParameters)
    
    return df

def addJoinKey(checkResult_df):
    df = checkResult_df
    
    df = (df
          .withColumn("name",F.regexp_extract("constraint","(\(\w+\()", 1))
          .withColumn("instance",F.regexp_extract("constraint","(\(\w+\,)", 1))
          .withColumn("name",F.regexp_replace("name","\(", ""))
          .withColumn("instance",F.regexp_replace("instance","[(,]", ""))
          .withColumn("instance",F.when(F.col("instance")=="",F.lit("*")).otherwise(F.col("instance")))
         )
    return df

def addFormatedDate(successMetrics_df, dataset_date):
    df = successMetrics_df
    
    dt_obj = datetime.fromtimestamp(int(dataset_date/1000))
    dateYYMMDD = str(dt_obj.date())
    df = (df.withColumn("YY_MM_DD",F.lit(dateYYMMDD)))
    
    return df


    
def transformCheckResult(checkResult_df, checkParameters):
    checkResult_df = addCheckConfigs(checkResult_df, checkParameters)
    checkResult_df = addJoinKey(checkResult_df)
    
    return checkResult_df
def transformSuccesMetrics(successMetrics_df, resultKeyParameters):
    successMetrics_df = addResultKey(successMetrics_df, resultKeyParameters)
    successMetrics_df = addFormatedDate(successMetrics_df, resultKeyParameters["dataset_date"])
    
    return successMetrics_df

def getConsolidatedDataframe(successMetrics_df, checkResult_df, verificationSuiteConfigs):
    """
    :param successMetrics:successMetrics Dataframe
    :param checkResult:checkResult Dataframe
    :param verificationSuiteConfigs: dictionary containing verificationSuite configs
    """
    checkParameters=verificationSuiteConfigs["checkParameters"]
    resultKeyParameters=verificationSuiteConfigs["resultKeyParameters"]
    
    checkResult_df = transformCheckResult(checkResult_df, checkParameters)
    successMetrics_df = transformSuccesMetrics(successMetrics_df, resultKeyParameters)
    
    consolidatedOutput = successMetrics_df.join(checkResult_df, on=["instance", "name"])
    
    return consolidatedOutput

def getConsolidatedOutput2(spark, currResult, vsYamlPath):
    successMetrics_df = VerificationResult.successMetricsAsDataFrame(spark, currResult)
    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, currResult)

    vsConfigs = yVS.getVerificationSuiteConfigs(vsYamlPath)
    vsConfigs["resultKeyParameters"]["dataset_date"] = ResultKey.current_milli_time()

    consolidatedOutput = getConsolidatedDataframe(successMetrics_df,
                                                       checkResult_df,
                                                       vsConfigs)
    return consolidatedOutput

def getSuccessMetrics(consolidatedOutput):
    metricColumns=["entity","instance","name","value","dataset_date","YY_MM_DD","tags"]
    successMetrics = consolidatedOutput.select(*[metricColumns])
    return successMetrics

def getCheckResult(consolidatedOutput):
    checkResultColumns=["check","check_level",
                    "check_status","constraint",
                    "constraint_status","constraint_message"]
    checkResult = consolidatedOutput.select(*[checkResultColumns])
    return checkResult

################################################################################################################
#                                  New Consolidate Approach
################################################################################################################

def addVsConfigs(df, vsConfigs, dfType):
    df = addDatasetDate(df, vsConfigs["resultKeyParameters"])
    df = addFormatedDate(df, vsConfigs["resultKeyParameters"]["dataset_date"])
    
    if dfType == "successMetrics":
        df = addKeyTags(df, vsConfigs["resultKeyParameters"])
    elif dfType == "checkResult":
        df = addCheckConfigs(df, vsConfigs["checkParameters"])
    
    return df

def setColumnsOrder(df, dfType):
    if dfType == "successMetrics":
        successColumns=['entity', 'instance', 'name', 'value', 'dataset_date', 'YY_MM_DD', 'tags']
        df = df.select(successColumns)
    elif dfType == "checkResult":
        checkColumns=['check','check_level','check_status','constraint',
                     'constraint_status','constraint_message','dataset_date','YY_MM_DD']
        df = df.select(checkColumns)
        
    return df

def addCheckStatus(successMetrics_df, checkResult_df):
    """
    Read checkResult_df and add information to successMetrics_df 
    """
    checkStatus_value=""
    first_failure = checkResult_df.filter(F.col("constraint_status")=="Failure").first()
    if first_failure == None:
        checkStatus_value="Success"
    else:
        check_level = checkResult_df.select("check_level").first()[0]
        checkStatus_value= check_level

    df = successMetrics_df.withColumn("check_status", F.lit(checkStatus_value))
    
    return df

def getOuputDFs(spark, currResult, vsYamlPath):
    print("getOuputDFs...")
    successMetrics_df = VerificationResult.successMetricsAsDataFrame(spark, currResult)
    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, currResult)

    vsConfigs = yVS.getVerificationSuiteConfigs(vsYamlPath)
    vsConfigs["resultKeyParameters"]["dataset_date"] = ResultKey.current_milli_time()
    
    successMetrics_df = addVsConfigs(successMetrics_df, vsConfigs, "successMetrics")
    checkResult_df = addVsConfigs(checkResult_df, vsConfigs, "checkResult")
    
    successMetrics_df = addCheckStatus(successMetrics_df, checkResult_df)
    
    return successMetrics_df, checkResult_df


    