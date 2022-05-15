from dataquality_bnr.yamlHandler import infrastructure as yI

######################################################################
#                             Reading
######################################################################

def readPersistentRepository(spark, hdfsPath):
    try:
        df = spark.read.parquet(hdfsPath)
        return df
    except:
        return None
    
def evaluateColumns(df):
    metricColumns=["entity","instance","name","value","dataset_date","YY_MM_DD","tags"]
    hasNeededColumns = all(column in df.columns for column in metricColumns)
    if hasNeededColumns == False:
        raise ValueError("The persistentRepository defined does not have the necessary columns.")
    
def getPersistentRepository(spark, view_path):  
    print("getPersistentRepository...")
    
    hdfsPath_successMetrics = view_path+"/persistentRepository/successMetrics.parquet"
    df = readPersistentRepository(spark, hdfsPath_successMetrics)
    if df!=None:
        evaluateColumns(df)
    
    return df


######################################################################
#                             Writing
######################################################################

def appendToPersistentMetrics(consolidatedOutput, infraYaml):
    hdfsPath = yI.getPersistentPath(infraYaml)
    consolidatedOutput.write.mode('append').parquet(hdfsPath)

def writeSuccessMetrics(successMetrics_df, view_path):
    hdfsPath_successMetrics = view_path+"/persistentRepository/successMetrics.parquet"
    successMetrics_df.write.mode('append').parquet(hdfsPath_successMetrics)
    return hdfsPath_successMetrics
    
def writeCheckResult(checkResult_df, view_path):
    hdfsPath_checkResult = view_path+"/persistentRepository/checkResult.parquet"   
    checkResult_df.write.mode('append').parquet(hdfsPath_checkResult)
    return hdfsPath_checkResult
    