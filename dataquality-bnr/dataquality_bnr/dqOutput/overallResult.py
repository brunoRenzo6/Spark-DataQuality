from dataquality_bnr.csvHandler import writer as csvWriter
from dataquality_bnr.dqOutput import main as dqOutput
from dataquality_bnr.yamlHandler import verificationSuite as yVS

from pyspark.sql import functions as F


#In this file there is a set of funtions used at overallResult.
#All results from dfirrent views are analyzed, turned into csv's and grouped in a single final csv

def buildCheckResultDf(spark, dfType):
    columns = ["check","check_level","check_status",
               "constraint","constraint_status","constraint_message",
               "dataset_date","YY_MM_DD"]
    data = [("","","",
             "","","",
             "","")]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)
        
    if dfType=="CheckResultNotFound":
        df = df.withColumn("constraint_message", F.lit("CheckResultNotFound"))
        df = df.withColumn("check_status", F.lit("Error"))
        
    elif dfType=="CheckResultEmptyOK":
        df = df.withColumn("constraint_message", F.lit(""))
        df = df.withColumn("check_status", F.lit("Success"))
        
    return df

def getCheckResultDf(spark, dqView):
    viewParquet_path = dqView.view_path+"/currentResult/checkResult.parquet"
    df=None
    try:
        df = spark.read.parquet(viewParquet_path)
        
        return df
    except:
        print()
        print("'"+dqView.viewName+"' checkResult not found in: "+viewParquet_path)
        df = buildCheckResultDf(spark, dfType="CheckResultNotFound")
        
        return df

    
def truncateErrorMessage(df):
    """
    When df contains complex log messages, it can mess up with df.write.csv() execution.
    To prevent it from happening, we truncate the df.constraint_message column.
    """

    df_truncate=(df
                 .withColumn("constraint_message",
                             F.when(F.col("constraint_message").contains("org.apache.spark"),
                                    F.lit("org.apache.spark...."))
                             .otherwise(F.col("constraint_message"))))
    return df_truncate
    
def writeResultCsv(spark, df, writePath, name, judgement):
    writeMode="overwrite"
    
    csvName = name+"_"+judgement+".csv"
    
    df=truncateErrorMessage(df)
    csvWriter.writeSingleCsvFile(spark, df, writeMode, writePath, csvName)
    
    return writePath+csvName

def unionToOverall(overall_df, view_df, dqView, judgement):
    view_df = view_df.withColumn("viewName", F.lit(dqView.viewName))
    view_df = view_df.withColumn("viewPath", F.lit(dqView.view_path))
    
    if overall_df==None:
        overall_df=view_df
    else:
        overall_df=overall_df.union(view_df)
            
    return overall_df


def getJudgement(view_df):
    judgement="NOK"
    
    has_error_filter = (F.col("check_status")=="Error")
    view_df=view_df.filter(has_error_filter)
    if view_df.head(1)==[]:
        judgement="OK"
    return  judgement

            

def getOverallResult(spark, dqViews, main_path, r):
    sc=spark.sparkContext
    overall_df=None
    overall_judgement=[]
    
    #viewName_list = list(dict.fromkeys(viewName_list))
    
    for viewName in dqViews:
        dqView=dqViews[viewName]
        viewParquet_path = dqView.view_path+"/currentResult/checkResult.parquet"
        viewCsv_path = dqView.view_path+"/currentResult/csvResult/"

        overallCsv_path = main_path+"overall/currentResult/csvResult/"

        view_df = getCheckResultDf(spark, dqView)
        has_failure_filter = (F.col("constraint_status")=="Failure")
        view_df = view_df.filter(has_failure_filter)

        
        vsConfigs = yVS.getVerificationSuiteConfigs(dqView.vsYaml)
        forceApprove = vsConfigs["checkParameters"]["forceApprove"]
        if forceApprove==False:
            judgement = getJudgement(view_df)
        else:
            judgement = "OK"
        
        #if view_df.head(1)==[]:
        #    view_df = buildCheckResultDf(spark, dfType="CheckResultEmptyOK")
            
        writePath= dqView.view_path+"/currentResult/csvResult/"
        
        r_p1 = writeResultCsv(spark, view_df, writePath, viewName, judgement)
        r = dqOutput.buildReturn("ovResult", viewName, r=r, p1=r_p1)

        overall_df=unionToOverall(overall_df, view_df, dqView, judgement)
        overall_judgement.append(judgement)

    print()
    #judgement = getJudgement(overall_df)
    
    if "NOK" in overall_judgement:
        overall_judgement="NOK"
    else:
        overall_judgement="OK"
    
    if overall_judgement == "OK":
        if overall_df.head(1)==[]:
            overall_df = buildCheckResultDf(spark, dfType="CheckResultEmptyOK")
            overall_df = overall_df.withColumn("viewName", F.lit("overall"))
            overall_df = overall_df.withColumn("viewPath", F.lit(""))

    writePath= main_path+"overall/currentResult/csvResult/"
    r_p1=writeResultCsv(spark, overall_df, writePath, "overall", judgement=overall_judgement)
    r = dqOutput.buildReturn("ovResult", "overall", r=r, p1=r_p1)

    return r
    
    