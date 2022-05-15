from pydeequ.analyzers import *
from pydeequ.anomaly_detection import *
from pydeequ.repository import *
from pydeequ.verification import *

from pyspark.sql import functions as F

from dataquality_bnr.yamlHandler import verificationSuite as yVS

# Series of functoins related specifically to custom_Histogram processment
# custom_Histogram its a workaround to enable AnomalyDetection over Histograms, which is not possible directly via Pydeequ.

def getAnalysisResult(analysisRunner):
    return analysisRunner.run()

def addAllHistograms(analysisRunner, columns):
    for c in columns:
        analysisRunner = analysisRunner.addAnalyzer(eval("Histogram("+c+")"))
    return analysisRunner

def getAnalysisRunner(spark, df):
    analysisRunner = (AnalysisRunner(spark)
                      .onData(df))
    return analysisRunner
    
def get_histogram_columns(adParameters):
    columns=[]
    for d in adParameters:
        anomalyDetection = d["addAnomalyCheck"]
        if "custom_Histogram" in anomalyDetection["analyzer"]:
            column=anomalyDetection["analyzer"].replace("custom_Histogram","")
            column=column.replace("(","").replace(")","")
            columns.append(column)
    return columns

def get_df_histogram(spark, df, columns):
    analysisRunner=getAnalysisRunner(spark, df)
    analysisRunner=addAllHistograms(analysisRunner, columns)    
    analysisResult=getAnalysisResult(analysisRunner)

    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
    return analysisResult_df
    
def transform_df_histogram(analysisResult_df):
    ratio_filtering=(F.substring('name', 0,16)=="Histogram.ratio.")
    filtered_df=analysisResult_df.filter(ratio_filtering)
    
    renamed_df = filtered_df.withColumn("name", F.regexp_replace('name', '\.', '_'))
    renamed_df = renamed_df.withColumn("instance", F.concat(F.col("instance"), F.lit("_")))
    renamed_df = renamed_df.withColumn("name", F.concat(F.col("instance"), F.col("name")))
    
    return renamed_df

def get_column_oriented_df(transformed_df):
    pivoted_df = transformed_df.drop("entity","instance")
    pivoted_df = pivoted_df.groupBy().pivot("name").agg(F.sum("value"))
    
    return pivoted_df

def get_pivoted_histogram(spark, df, columns):
    analysisResult_df = get_df_histogram(spark, df, columns)
    transformed_df = transform_df_histogram(analysisResult_df)
    pivoted_df = get_column_oriented_df(transformed_df)

    return pivoted_df



def getVerificationRunBuilder(spark, pivoted_df, repository):
    vRunBuilder = (VerificationSuite(spark)
               .onData(pivoted_df)
               .useRepository(repository))
    
    return vRunBuilder

def addAnomalyChecks(vRunBuilder, adParameters, pivoted_df_columns):
    for d in adParameters:
        anomalyDetection = d["addAnomalyCheck"]
        if "custom_Histogram" in anomalyDetection["analyzer"]:
            column=anomalyDetection["analyzer"].replace("custom_Histogram","")
            column=column.replace("(","").replace(")","")

            for p_df_column in pivoted_df_columns:
                if column.replace('"','') in p_df_column:
                    analyzer= Mean(p_df_column)
                    strategy = eval(anomalyDetection["strategy"])
                    
                    vRunBuilder = vRunBuilder.addAnomalyCheck(strategy, analyzer)
    return vRunBuilder

def custom_runAnomalyDetection(spark, pivoted_df, repository, adParameters):
    vRunBuilder = getVerificationRunBuilder(spark, pivoted_df, repository)
    vRunBuilder = addAnomalyChecks(vRunBuilder, adParameters, pivoted_df.columns)
    currResult = vRunBuilder.run()
    
    return currResult

def union_custom_df(regular_df, custom_df):
    """
    Union both df's and setting check_status as a constant, either [Success, Warning or Error]
    """
    
    regular_df=regular_df.union(custom_df)
    filtered_df = regular_df.filter(F.col("check_status")!="Success")
    first_row =filtered_df.select("check_status").first()
    if first_row!=None:
        check_status = first_row[0]
    else:
        check_status = "Success"
    
    regular_df=regular_df.withColumn("check_status", F.lit(check_status))
    
    return regular_df

def unionWithRegular_successMetrics(successMetrics_df, custom_successMetrics_df):
    successMetrics_df=successMetrics_df.union(custom_successMetrics_df)
    
    
def custom_getAnomalyDetection(spark, df, vsYamlPath, repository):
    print("custom_getAnomalyDetection...")
    
    adParameters = yVS.getADParameters(vsYamlPath)
    
    columns=get_histogram_columns(adParameters)
    if len(columns)>0:
        
        pivoted_df = get_pivoted_histogram(spark, df, columns)
        currResult = custom_runAnomalyDetection(spark, pivoted_df, repository, adParameters)
        
        return currResult
    else:
        return None
